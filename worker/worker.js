// BLADE Fleet Ops event worker
// Runs on Cloudflare Workers. Every minute it samples the ADS-B feeds twice, detects departures and arrivals at
// BLADE locations, posts them to Slack, and serves the shared event log + last-known positions to the dashboard.
// Detection logic mirrors index.html so both agree on what counts as an event.
// The fleet list lives in KV (seeded from TAILS below) and is managed by @-mentioning the bot in Slack.

const TAILS=[
  {hex:'acbcfd',label:''},{hex:'a81624',label:''},{hex:'ac1dbc',label:''},
  {hex:'a928da',label:''},{hex:'aa8548',label:''},{hex:'a5e880',label:''},
  {hex:'a2a484',label:''},{hex:'ab7da4',label:''},{hex:'a45915',label:''},
  {hex:'aa0cf4',label:'N747EE'},{hex:'a07ea1',label:'N1308'},
  {hex:'a7143e',label:'N555ZA'},{hex:'aa416f',label:'N76ZA'},
];
const DEFAULT_LOCATIONS=[ // fallback only; the live list comes from locations.json in the repo (see getLocations)
  {id:'JRB',name:'Downtown Manhattan HP',lat:40.7011,lon:-74.0090,r:0.5},
  {id:'E34',name:'East 34th St Heliport',lat:40.7428,lon:-73.9722,r:0.5},
  {id:'W30',name:'West 30th St Heliport',lat:40.7544,lon:-74.0072,r:0.5},
  {id:'65NJ',name:'Helo Kearny (65NJ)',lat:40.7315,lon:-74.1168,r:0.5},
  {id:'TEB',name:'Teterboro (TEB)',lat:40.8501,lon:-74.0608,r:2.0},
  {id:'HPN',name:'Westchester (HPN)',lat:41.0670,lon:-73.7076,r:2.0},
  {id:'JFK',name:'JFK International',lat:40.6413,lon:-73.7781,r:2.0},
  {id:'EWR',name:'Newark (EWR)',lat:40.6895,lon:-74.1745,r:2.0},
  {id:'LGA',name:'LaGuardia (LGA)',lat:40.7769,lon:-73.8740,r:2.0},
  {id:'BED',name:'Bedford (BED)',lat:42.4700,lon:-71.2893,r:1.5},
  {id:'FRG',name:'Republic (FRG)',lat:40.7288,lon:-73.4134,r:1.5},
  {id:'HTO',name:'East Hampton (HTO)',lat:40.9598,lon:-72.2518,r:1.5},
  {id:'MTP',name:'Montauk (MTP)',lat:41.0765,lon:-71.9208,r:1.5},
  {id:'SWF',name:'Stewart (SWF)',lat:41.5041,lon:-74.1048,r:2.0},
  {id:'ACY',name:'Atlantic City (ACY)',lat:39.4576,lon:-74.5772,r:2.0},
  {id:'PVD',name:'Providence (PVD)',lat:41.7240,lon:-71.4283,r:2.0},
  {id:'BOS',name:'Boston Logan (BOS)',lat:42.3656,lon:-71.0096,r:2.0},
  {id:'PHL',name:'Philadelphia (PHL)',lat:39.8729,lon:-75.2437,r:2.0},
  {id:'IAD',name:'Dulles (IAD)',lat:38.9531,lon:-77.4565,r:2.0},
  {id:'MIA',name:'Miami (MIA)',lat:25.7959,lon:-80.2870,r:2.0},
  {id:'PBI',name:'Palm Beach (PBI)',lat:26.6832,lon:-80.0956,r:2.0},
  {id:'HYA',name:'Hyannis (HYA)',lat:41.6693,lon:-70.2803,r:1.5},
  {id:'ACK',name:'Nantucket (ACK)',lat:41.2531,lon:-70.0602,r:1.5},
  {id:'MVY',name:"Martha's Vineyard (MVY)",lat:41.3931,lon:-70.6154,r:1.5},
  {id:'LWM',name:'Lawrence (LWM)',lat:42.7172,lon:-71.1233,r:1.5},
];
let LOCATIONS=DEFAULT_LOCATIONS;
const EVENT_ALT=500,MAX_NEAR_NM=10;
const LOCATIONS_URL='https://rmccll9-wq.github.io/blade-fleet-ops/locations.json',LOCATIONS_EVERY_MS=5*60e3;
const WAYPOINTS_URL='https://rmccll9-wq.github.io/blade-fleet-ops/waypoints.json';let WAYPOINTS=[];
const LOST_SEC=60,ARRIVE_MAX_ALT=1500,DEPART_MAX_ALT=2500,EVENT_GAP_SEC=300,LK_MAX_H=48; // LOST_SEC: seconds without a position report before a tail counts as no longer tracked
const FEEDS=['https://api.adsb.lol/v2/icao/','https://opendata.adsb.fi/api/v2/icao/'];
const TRACE_BASE='https://adsb.lol/data/traces/',TRACE_EVERY_MS=5*60e3;
// The feeds rate-limit / block Cloudflare's shared egress IPs, so upstream requests go through the Vercel proxy
// (same one the dashboard uses). It only forwards to the allow-listed feed hosts.
const PROXY='https://blade-fleet-ops.vercel.app/api/proxy?u=';
function upstream(u,ms){return fetch(PROXY+encodeURIComponent(u),{headers:{'Accept':'application/json'},signal:AbortSignal.timeout(ms||10000)});}
const MAX_EVENTS=1000,MAX_LEGS=3000,SAMPLES_PER_RUN=4,SAMPLE_GAP_MS=15000,SAMPLE_CUTOFF_MS=50000; // sample every 15 s (0/15/30/45 s); never start one after 50 s so a run can't overlap the next minute's
const TZ='America/New_York';

export default {
  async scheduled(event,env,ctx){ctx.waitUntil(runSweeps(env));ctx.waitUntil(kickGroundConnect(env));},
  async fetch(req,env,ctx){
    const url=new URL(req.url);
    const cors={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET','Cache-Control':'no-store','Content-Type':'application/json'};
    if(req.method==='OPTIONS')return new Response(null,{headers:cors});
    if(url.pathname==='/slack/events'&&req.method==='POST')return handleSlack(req,env,ctx);
    if(url.pathname==='/slack/whoami'){ // diagnostics: which bot identity the token belongs to (no secrets returned)
      if(!env.SLACK_BOT_TOKEN)return new Response(JSON.stringify({error:'SLACK_BOT_TOKEN not set'}),{headers:cors});
      const r=await fetch('https://slack.com/api/auth.test',{method:'POST',headers:{'Authorization':'Bearer '+env.SLACK_BOT_TOKEN}});
      const j=await r.json();
      return new Response(JSON.stringify({ok:j.ok,error:j.error,bot_user:j.user,bot_user_id:j.user_id,team:j.team,signing_secret_set:!!env.SLACK_SIGNING_SECRET}),{headers:cors});
    }
    if(url.pathname==='/slack/selftest'){ // diagnostics: try posting to a channel by name; the Slack error says if the bot is not a member
      const ch=url.searchParams.get('channel');if(!ch)return new Response(JSON.stringify({error:'pass ?channel=name'}),{headers:cors});
      const r=await fetch('https://slack.com/api/chat.postMessage',{method:'POST',headers:{'Content-Type':'application/json; charset=utf-8','Authorization':'Bearer '+env.SLACK_BOT_TOKEN},
        body:JSON.stringify({channel:ch.startsWith('#')?ch:'#'+ch,text:':white_check_mark: BLADE Ops Agent can post here. Mention me with `list`, `add N12345 [label]`, `remove N12345`, or `status`.'})});
      const j=await r.json();return new Response(JSON.stringify({ok:j.ok,error:j.error,channel:j.channel}),{headers:cors});
    }
    if(url.pathname==='/locations')return new Response(JSON.stringify(await getLocations(env)),{headers:cors});
    if(url.pathname==='/legs')return new Response(JSON.stringify(await getLegs(env)),{headers:cors});
    if(url.pathname==='/recap'){ // same text the Slack bot posts; ?tail=N84BL&period=today|yesterday|24h
      await getLocations(env);const tails=await getTails(env);
      const txt=await recapText(env,{tail:url.searchParams.get('tail')||null,period:url.searchParams.get('period')||'today'},tails);
      return new Response(txt,{headers:{...cors,'Content-Type':'text/plain; charset=utf-8'}});
    }
    if(url.pathname==='/wx')return new Response(JSON.stringify({stations:await getStations(env),state:(await env.KV.get('wx_state','json'))||null}),{headers:cors});
    if(url.pathname==='/wx/setup'){ // one-time: post a hello into the weather channel by name and return its ID for wrangler.toml
      const ch=url.searchParams.get('channel');if(!ch)return new Response(JSON.stringify({error:'pass ?channel=name'}),{headers:cors});
      const r=await fetch('https://slack.com/api/chat.postMessage',{method:'POST',headers:{'Content-Type':'application/json; charset=utf-8','Authorization':'Bearer '+env.SLACK_BOT_TOKEN},
        body:JSON.stringify({channel:(ch.startsWith('#')||/^[CG][A-Z0-9]{8,}$/.test(ch))?ch:'#'+ch,text:':cloud: Flight-category watch connected. I post when a station changes between VFR, MVFR, IFR and LIFR. Say `list` for the current picture, `add KHVN` or `remove KISP` to change the stations.'})});
      const j=await r.json();return new Response(JSON.stringify({ok:j.ok,error:j.error,channel:j.channel}),{headers:cors});
    }
    if(url.pathname==='/slack/corrections'){ // read-only diagnostics: struck-through (corrected) posts in the fleet channel, last 48 h
      const days=Math.min(14,+(url.searchParams.get('days')||2)),q=url.searchParams.get('q')||'^~';
      const r=await fetch('https://slack.com/api/conversations.history?'+new URLSearchParams({channel:env.SLACK_CHANNEL_ID,oldest:String((Date.now()-days*86400e3)/1000),limit:'999'}),{headers:{'Authorization':'Bearer '+env.SLACK_BOT_TOKEN}});
      const j=await r.json();if(!j.ok)return new Response(JSON.stringify({error:j.error}),{headers:cors});
      const re=new RegExp(q,'i');
      return new Response(JSON.stringify((j.messages||[]).filter(m=>re.test(m.text||'')).map(m=>({ts:m.ts,edited:m.edited&&m.edited.ts,user:m.user,bot:!!m.bot_id,text:m.text}))),{headers:cors});
    }
    if(url.pathname==='/fixlog')return new Response(JSON.stringify({ttn:await env.KV.get('fix_2026-11-17_N92N_TTN'),withdrawals:await env.KV.get('fix_2026-11-05_withdrawals'),v1:await env.KV.get('fix_2026-10-30_N76ZA'),v2:await env.KV.get('fix_2026-10-30_N76ZA_v2'),v3:await env.KV.get('fix_2026-10-30_N76ZA_v3')}),{headers:cors});
    if(url.pathname==='/tails')return new Response(JSON.stringify(await getTails(env)),{headers:cors});
    if(url.pathname==='/events')return new Response(JSON.stringify((await env.KV.get('events','json'))||[]),{headers:cors});
    if(url.pathname==='/state')return new Response(JSON.stringify((await env.KV.get('state','json'))||{}),{headers:cors});
    if(url.pathname==='/debug'){
      const batch=(await getTails(env)).map(t=>t.hex).join(',').toUpperCase(),res={};
      for(const base of FEEDS){try{const r=await upstream(base+batch,12000);res[base]={status:r.status,body:(await r.text()).slice(0,160)};}catch(e){res[base]={error:String(e)};}}
      return new Response(JSON.stringify(res),{headers:cors});
    }
    if(url.pathname==='/health')return new Response(JSON.stringify((await env.KV.get('health','json'))||{lastRun:null}),{headers:cors});
    return new Response('BLADE Fleet Ops event worker. Endpoints: /events /state /tails /locations /health',{headers:{'Content-Type':'text/plain'}});
  }
};

async function runSweeps(env){
  const start=Date.now();
  for(let i=0;i<SAMPLES_PER_RUN;i++){
    const at=start+i*SAMPLE_GAP_MS; // fixed offsets in the minute, so a slow feed call doesn't push later samples back
    if(at-start>SAMPLE_CUTOFF_MS||Date.now()-start>SAMPLE_CUTOFF_MS)break;
    if(Date.now()<at)await new Promise(r=>setTimeout(r,at-Date.now()));
    const t0=Date.now();
    try{await sweep(env,i%2===0);}catch(e){console.error('sweep failed',e&&e.message);} // Slack channels read twice a minute (samples 1 and 3)
    console.log(`sample ${i+1} at +${Math.round((t0-start)/1000)}s took ${Math.round((Date.now()-t0)/1000)}s${i%2===0?' (with Slack read)':''}`);
  }
}

// ---- geometry / status (same as index.html) ----
function haversineNm(la1,lo1,la2,lo2){const R=3440.065,r=Math.PI/180;const a=Math.sin((la2-la1)*r/2)**2+Math.cos(la1*r)*Math.cos(la2*r)*Math.sin((lo2-lo1)*r/2)**2;return R*2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));}
function nearestLoc(lat,lon){if(lat==null||lon==null)return null;let best=null,bd=Infinity;for(const L of LOCATIONS){const d=haversineNm(lat,lon,L.lat,L.lon);if(d<bd){bd=d;best=L;}}if(bd>MAX_NEAR_NM)return null;return{loc:best,dist:bd};}
function snapNear(n){return n?{id:n.loc.id,name:n.loc.name,r:n.loc.r,dist:n.dist}:null;}
function nearForEvent(s){return !!s&&s.dist<=s.r+(s.r<1?0.5:1.0);}
function getStatus(ac){const age=ac.seen_pos!=null?ac.seen_pos:ac.seen;if(!ac.lat||age>LOST_SEC)return'stale';if(ac.alt_baro==='ground')return'ground';if(typeof ac.alt_baro==='number'&&ac.alt_baro>0)return'airborne';if(ac.gs>30)return'airborne';return'ground';}
function altOf(ac){return typeof ac.alt_baro==='number'?ac.alt_baro:(ac.alt_baro==='ground'?0:null);}
function lkFresh(p,now){return !!p&&!!p.lastSeen&&(now-p.lastSeen)<LK_MAX_H*3600e3;}
function _nL(rem){if(rem==0)return"";--rem;return _LA[rem];}
function _nLL(rem){if(rem==0)return"";--rem;return _LA[Math.floor(rem/25)]+_nL(rem%25);}
function hexToN(hex){
  let off=parseInt(hex,16)-0xA00001; if(isNaN(off)||off<0||off>=915399)return null;
  let reg="N"+(Math.floor(off/101711)+1); off%=101711;
  if(off<=600)return reg+_nLL(off); off-=601;
  reg+=Math.floor(off/10111); off%=10111;
  if(off<=600)return reg+_nLL(off); off-=601;
  reg+=Math.floor(off/951); off%=951;
  if(off<=600)return reg+_nLL(off); off-=601;
  reg+=Math.floor(off/35); off%=35;
  if(off<=24)return reg+_nL(off); off-=25;
  return reg+off;
}
const _LA="ABCDEFGHJKLMNPQRSTUVWXYZ";
function _lv(L){if(!L)return 0;const a=_LA.indexOf(L[0]);if(a<0)return -1;if(L.length===1)return a*25+1;const b=_LA.indexOf(L[1]);return b<0?-1:a*25+b+2;}
function nToHex(n){ // US N-number -> ICAO hex (inverse of hexToN)
  const m=/^N([1-9])(\d?)(\d?)(\d?)(\d?)([A-Z]{0,2})$/.exec((n||'').toUpperCase().replace(/[^A-Z0-9]/g,''));if(!m)return null;
  const d=[m[1],m[2],m[3],m[4],m[5]],L=m[6];if((d[4]&&L)||/[IO]/.test(L))return null;
  const hx=o=>(0xA00001+o).toString(16);
  let off=(+d[0]-1)*101711,v;
  if(!d[1]){v=_lv(L);return v<0?null:hx(off+v);}off+=601+(+d[1])*10111;
  if(!d[2]){v=_lv(L);return v<0?null:hx(off+v);}off+=601+(+d[2])*951;
  if(!d[3]){v=_lv(L);return v<0?null:hx(off+v);}off+=601+(+d[3])*35;
  if(!d[4]){if(L.length>1)return null;v=L?_LA.indexOf(L)+1:0;return(L&&v<1)?null:hx(off+v);}
  return hx(off+25+(+d[4]));
}
function regOf(hex,ac,label){return (ac&&ac.r&&ac.r.trim())||hexToN(hex)||label||hex.toUpperCase();}

// ---- events ----
// ===== Departure / arrival rules =====
// A departure is HELD when a tail lifts off near a location and only posted once it is confirmed:
//   it leaves the location's event ring, OR climbs 300 ft above its on-pad reference, OR is airborne 2 min and has moved 0.3 nm.
// If it sets down again at the same location first (a reposition / hover-taxi), or goes quiet without leaving, nothing is posted.
// Arrivals fire only on touchdown (airborne -> ground) near a location, or when tracking is lost while low near one.
// Heights are above the location's field elevation, from pressure altitude corrected with the local METAR altimeter setting.
const TOUCHDOWN_CONFIRM_GS=60,DEP_CONFIRM_CLIMB_FT=300,DEP_CONFIRM_SEC=120,DEP_CONFIRM_MOVE_NM=0.3,LIFT_MEMORY_MS=15*60e3,AIRBORNE_AGL_FT=150,AIRBORNE_GS_KT=30;
// Touchdowns: must be inside the location's own radius and still on the ground, at 60 kt or less, at a later sample (or go quiet there).
// Going quiet in flight: it stopped only if the gap holds at least 3 minutes that the distance flown can't explain (landing, ground
// time and takeoff take at least that). Otherwise it kept flying: same flight, and any estimated arrival is withdrawn.
// A stop is placed only if the tail reappears low near a location.
// An estimated arrival also needs the tail to look like it was landing: not faster than 90 kt while above 500 ft.
// Estimated (signal-loss) arrivals: within 0.75 nm of a pad / 2.25 nm of an airport, at most 800 ft above a pad / 1,000 ft above an
// airport (altimeter-corrected), and not faster than 90 kt while above 300 ft. Real ones measured 2026-11-05: pads <=0.41 nm and <=375 ft,
// airports <=1.94 nm and <=525 ft (raw). Anything outside falls back to 'next seen parked' or route inference.
const LOSS_RING_PAD_NM=0.75,LOSS_RING_APT_NM=2.25,LOSS_MAX_AGL_PAD=800,LOSS_MAX_AGL_APT=1000,LANDING_FAST_MIN_AGL=300;
const STOP_MIN_MS=180e3,FLYTHROUGH_MIN_GS=40,FLYTHROUGH_MAX_GAP_MS=15*60e3,LANDING_MAX_GS=90,LANDING_FAST_MAX_AGL=500;
function touchdownLoc(near){return near&&near.dist<=near.r?near:null;}
// Where a tail that went quiet low was about to land: a location whose event ring holds its last position and which is either
// ahead of it (within 60° of its track) or right under it (inside the location's own radius). Nearest such location wins.
function landingCandidate(p){
  if(p.lat==null)return null;
  const useTrack=typeof p.track==='number'&&(p.gs||0)>=20;let best=null;
  for(const L of LOCATIONS){
    const d=haversineNm(p.lat,p.lon,L.lat,L.lon);if(d>(L.r<1||L.aux?LOSS_RING_PAD_NM:LOSS_RING_APT_NM))continue; // secondary airports (aux) get the tight pad limits
    if(useTrack&&d>L.r){const diff=Math.abs(((bearingDeg(p.lat,p.lon,L.lat,L.lon)-p.track)%360+540)%360-180);if(diff>60)continue;}
    if(!best||d<best.dist)best={id:L.id,name:L.name,r:L.r,aux:L.aux,dist:d};
  }
  return best;
}
// Time in a signal gap that the distance flown can't explain (optionally routing via a location it may have stopped at).
function unexplainedMs(prev,ac,now,via){
  const gap=now-prev.lastSeen;if(!(prev.gs>=FLYTHROUGH_MIN_GS))return gap;
  if(!via)return gap-haversineNm(prev.lat,prev.lon,ac.lat,ac.lon)/Math.max(prev.gs,ac.gs||0)*3600e3;
  // each side of the gap at its own measured speed: into the location at the speed it vanished at, out of it at the speed it reappeared at
  const vIn=Math.max(40,prev.gs),vOut=Math.max(40,ac.gs||0,prev.gs);
  return gap-(haversineNm(prev.lat,prev.lon,via.lat,via.lon)/vIn+haversineNm(via.lat,via.lon,ac.lat,ac.lon)/vOut)*3600e3;
}
// An estimated arrival is withdrawn only on clear evidence the tail never stopped: under a minute unexplained AND it kept its heading.
// Reversing course near the location (back the way it came) is what a quick drop-off looks like, so the arrival stands.
const WITHDRAW_MAX_UNEXPLAINED_MS=60e3,REVERSAL_DEG=120;
function passedOver(prev,ac,now,P){
  if(prev.lat==null||!(prev.gs>=FLYTHROUGH_MIN_GS))return false;
  const gap=now-prev.lastSeen;if(gap<=0||gap>FLYTHROUGH_MAX_GAP_MS)return false;
  if(typeof prev.track==='number'&&typeof ac.track==='number'&&Math.abs(((ac.track-prev.track)%360+540)%360-180)>REVERSAL_DEG)return false;
  return unexplainedMs(prev,ac,now,P)<WITHDRAW_MAX_UNEXPLAINED_MS;
}
// A stop nobody saw: the tail vanished in flight and reappeared having lost time we can't place. Try each location as a detour.
// It is logged (as estimated) only when exactly one location fits: ahead of the track when it vanished (within 45°), and the
// detour leaves 5-30 minutes of ground time at the average of its speeds before and after the gap.
const INFER_SPARE_MIN_MS=5*60e3,INFER_SPARE_MAX_MS=30*60e3,INFER_CONE_DEG=45;
function inferStop(prev,ac,now,exclude){
  if(prev.lat==null)return null;
  const gap=now-prev.lastSeen,spd=Math.max(60,((prev.gs||0)+(ac.gs||0))/2),useTrack=typeof prev.track==='number'&&(prev.gs||0)>=40;
  const fits=[];
  for(const L of LOCATIONS){
    if(L.aux||(exclude&&exclude.includes(L.id)))continue; // unseen stops are only inferred at core locations
    const d1=haversineNm(prev.lat,prev.lon,L.lat,L.lon),d2=haversineNm(L.lat,L.lon,ac.lat,ac.lon);
    if(d1<ringNm(L)||d2<ringNm(L))continue; // it was seen near this one: not an unseen stop
    if(useTrack){const diff=Math.abs(((bearingDeg(prev.lat,prev.lon,L.lat,L.lon)-prev.track)%360+540)%360-180);if(diff>INFER_CONE_DEG)continue;}
    const spare=gap-(d1+d2)/spd*3600e3;
    if(spare>=INFER_SPARE_MIN_MS&&spare<=INFER_SPARE_MAX_MS)fits.push({L,arr:prev.lastSeen+d1/spd*3600e3+60e3,dep:now-d2/spd*3600e3});
  }
  return fits.length===1?fits[0]:null;
}
function flewThrough(prev,ac,now,via){
  if(prev.lat==null||!(prev.gs>=FLYTHROUGH_MIN_GS))return false;
  const gap=now-prev.lastSeen;if(gap<=0||gap>FLYTHROUGH_MAX_GAP_MS)return false;
  return unexplainedMs(prev,ac,now,via||null)<STOP_MIN_MS;
}
const ELEV_FALLBACK={"JRB": 7, "E34": 10, "W30": 7, "65NJ": 11, "TEB": 9, "HPN": 439, "JFK": 13, "EWR": 18, "LGA": 21, "BED": 133, "FRG": 82, "HTO": 55, "MTP": 6, "SWF": 491, "ACY": 75, "PVD": 55, "BOS": 20, "PHL": 36, "IAD": 312, "MIA": 8, "PBI": 19, "HYA": 54, "ACK": 47, "MVY": 67, "LWM": 148, "N12": 43, "6NJ6": 20, "BLM": 153, "JY79": 133, "FOK": 67, "HWV": 81, "ISP": 99, "HVN": 12, "BDR": 9, "MMU": 187, "MSV": 1403, "29NY": 473, "W00": 168, "39N": 128, "CDW": 173, "LDJ": 23}; // field elevations (ft) from OurAirports, used until locations.json carries "elev"
function locById(id){return LOCATIONS.find(x=>x.id===id)||null;}
function elevOf(id){const L=locById(id);return L&&typeof L.elev==='number'?L.elev:(ELEV_FALLBACK[id]??0);}
function ringNm(s){return s.r+(s.r<1?0.5:1.0);}
// Altimeter setting (inHg) from the flight-category watch's METARs: nearest station within 60 nm, else the regional median.
function qnhFor(lat,lon,wx,now){
  const st=Object.values((wx&&wx.stations)||{}).filter(x=>typeof x.altim==='number'&&x.lat!=null&&now-(x.obs||0)<12*3600e3);
  if(!st.length||lat==null)return null;
  let best=null,bd=Infinity;for(const x of st){const d=haversineNm(lat,lon,x.lat,x.lon);if(d<bd){bd=d;best=x;}}
  if(bd<=60)return best.altim;
  const v=st.map(x=>x.altim).sort((a,b)=>a-b);return v[Math.floor(v.length/2)];
}
function indAlt(ac,qnh){if(ac.alt_baro==='ground')return'ground';if(typeof ac.alt_baro!=='number')return null;return ac.alt_baro+(qnh?(qnh-29.92)*1000:0);} // ft MSL
function aglAt(ind,locId){if(ind==='ground')return 0;if(ind==null)return null;return Math.round(ind-elevOf(locId));}
// Airborne vs on ground for detection. Transponder ground flag wins; otherwise over 30 kt, or more than 150 ft above the
// nearby location (150 ft clears the ±125 ft encoder tolerance, so a hover-taxi stays "on ground"). Away from locations, or with no
// altimeter setting available, falls back to the raw rule (pressure altitude above 0 ft).
function flightStatus(ac,near,ind,qnh){
  // an "on ground" flag above 40 kt anywhere but on a location's own surface is a bad reading: a helicopter can't be doing that
  if(ac.alt_baro==='ground')return (ac.gs||0)>40&&!touchdownLoc(near)?'airborne':'ground';
  if((ac.gs||0)>AIRBORNE_GS_KT)return'airborne';
  if(typeof ind!=='number')return'ground';
  if(qnh&&near&&nearForEvent(near))return ind-elevOf(near.id)>AIRBORNE_AGL_FT?'airborne':'ground';
  if(near&&nearForEvent(near))return'ground'; // near a location with no altimeter setting: raw altitude can be 600+ ft off, so go by speed alone (30 kt or less = on ground)
  return ac.alt_baro>0?'airborne':'ground';
}
function confirmDep(pend,ac,ind,now){
  const L=locById(pend.loc.id),off=L?haversineNm(ac.lat,ac.lon,L.lat,L.lon):Infinity;
  if(off>ringNm(pend.loc))return'left the location';
  if(typeof ind==='number'&&typeof pend.refInd==='number'&&ind-pend.refInd>=DEP_CONFIRM_CLIMB_FT)return'climbed 300 ft';
  if(now-pend.ts>=DEP_CONFIRM_SEC*1000&&haversineNm(ac.lat,ac.lon,pend.lat,pend.lon)>=DEP_CONFIRM_MOVE_NM)return'airborne 2 min and moving';
  return null;
}
// Returns the holds to carry forward: {dep, arr}.
function detectEvents(t,ac,prev,status,ind,near,now,events,out,qnh){
  const none={dep:null,arr:null};
  if(!prev)return none;
  // A touchdown seen on the last sample is real only if the tail is still on the ground now; airborne again means one bad
  // "on ground" reading in flight, or a touch-and-go. Either way: no arrival, and no departure either.
  if(prev.arrPend){
    if(status==='ground'&&(ac.gs||0)<=TOUCHDOWN_CONFIRM_GS){
      const ap=prev.arrPend;
      if(ap.replaces)(out.retract=out.retract||[]).push({hex:t.hex,ts:ap.replaces.ts,locId:ap.replaces.locId,why:'it landed at '+ap.loc.name+' instead'});
      pushEvent(ap.replaces?events.filter(e=>!(e.hex===t.hex&&e.type==='ARRIVED'&&e.ts===ap.replaces.ts)):events,out,'ARRIVED',t,ac,ap.loc,ap.ts,!!ap.inferred,prev);return none;}
    if(status==='ground')return{dep:null,arr:prev.arrPend}; // still rolling fast: wait for it to slow before calling it a landing
    return none;
  }
  if(prev.status==='nosignal'&&status==='ground'&&(prev.lostAirborne||prev.lossArr)){
    const td=touchdownLoc(near);
    // Held like any touchdown: it must still be on the ground, slowed to 60 kt or less, at the next sample.
    if(td&&prev.lossArr&&prev.lossArr.locId!==td.id) // the estimated arrival named the wrong place: move it to where the tail actually is
      return{dep:null,arr:{loc:td,ts:prev.lossArr.ts,gs:ac.gs||0,inferred:true,replaces:{ts:prev.lossArr.ts,locId:prev.lossArr.locId}}};
    if(td&&prev.lostAirborne){ // went quiet in flight, next seen on the ground here: it landed here. Time it by dead reckoning.
      let ts=now;if(prev.lat!=null&&prev.gs>=FLYTHROUGH_MIN_GS)ts=Math.min(now,prev.lastSeen+haversineNm(prev.lat,prev.lon,ac.lat,ac.lon)/prev.gs*3600e3+30e3);
      return{dep:null,arr:{loc:td,ts,gs:ac.gs||0,inferred:true}};
    }
    return none;
  }
  if(prev.status==='nosignal'&&status==='airborne'&&(prev.lostAirborne||prev.lossArr)){
    const lossLoc=prev.lossArr?locById(prev.lossArr.locId):null;
    if(lossLoc?passedOver(prev,ac,now,lossLoc):flewThrough(prev,ac,now)){ // it kept flying the whole time: same flight
      if(prev.lossArr)(out.retract=out.retract||[]).push({hex:t.hex,ts:prev.lossArr.ts,locId:prev.lossArr.locId,why:'it flew past without landing'});
      return none;
    }
    // it lost time somewhere. Which location could it have stopped at: where an arrival was estimated, or the one it's low near now?
    const a=near?aglAt(ind,near.id):null;
    const P=prev.lossArr?locById(prev.lossArr.locId):(nearForEvent(near)&&a!=null&&a<=DEPART_MAX_ALT?locById(near.id):null);
    if(!P){ // stopped somewhere it wasn't seen. If the route points to exactly one location, log that stop as estimated.
      const g=prev.lostAirborne?inferStop(prev,ac,now,near&&nearForEvent(near)?[near.id]:null):null;
      if(g){const loc={id:g.L.id,name:g.L.name,r:g.L.r,dist:0};
        pushEvent(events,out,'ARRIVED',t,prev,loc,Math.round(g.arr),true,prev);
        const k=out.length;pushEvent([...events,...out],out,'DEPARTED',t,ac,loc,Math.round(Math.max(g.dep,g.arr+60e3)),true,prev);
        for(const e of out.slice(k-1))if(e.locId===loc.id)e.route=true;}
      return none;
    }
    if(!prev.lossArr&&flewThrough(prev,ac,now,P)){ // routing over P accounts for the time: it passed over P without stopping
      if(prev.lossArr)(out.retract=out.retract||[]).push({hex:t.hex,ts:prev.lossArr.ts,locId:prev.lossArr.locId,why:'it flew past without landing'});
      return none;
    }
  }
  let pend=prev.depPend||null;
  if(pend){
    if(status==='airborne'){
      if(confirmDep(pend,ac,ind,now)){pushEvent(events,out,'DEPARTED',t,ac,pend.loc,pend.ts,pend.inferred,prev);return none;}
      if(now-pend.ts>=DEP_CONFIRM_SEC*1000)pend={...pend,ts:now}; // still hovering in place: restart the clock
      return{dep:pend,arr:null};
    }
    if(status==='ground'){
      if(near&&near.id===pend.loc.id&&touchdownLoc(near))return none; // set down again on the same location: repositioning, not a flight
      pushEvent(events,out,'DEPARTED',t,ac,pend.loc,pend.ts,pend.inferred,prev); // touched down somewhere else: the departure was real
      const td=touchdownLoc(near);return{dep:null,arr:td?{loc:td,ts:now,gs:ac.gs||0}:null};
    }
  }
  if((prev.status==='ground'||prev.status==='nosignal')&&status==='airborne'){
    let from=null;
    if(prev.status==='ground'){if(touchdownLoc(prev.nearest))from=prev.nearest;}            // was on the ground AT a location
    else if(prev.lostAirborne)from=nearForEvent(near)?near:null;                              // stopped out of coverage, now low near one: it stopped there
    else if(prev.lossArr)from=locById(prev.lossArr.locId)?{...snapNear({loc:locById(prev.lossArr.locId),dist:0}),dist:0}:null; // departing where it was estimated to land
    else if(nearForEvent(prev.nearest)&&lkFresh(prev,now))from=prev.nearest;                  // went quiet at a location
    if(!from&&prev.status==='nosignal'){const a=near?aglAt(ind,near.id):null;if(a!=null&&a<=DEPART_MAX_ALT&&nearForEvent(near))from=near;}
    if(!from)return none;
    // On-pad reference altitude: the tail's own last reading on the ground there (cancels encoder error), else field elevation if QNH is known.
    const refInd=(prev.status==='ground'&&typeof prev.ind==='number'&&prev.nearest&&prev.nearest.id===from.id)?prev.ind:(qnh?elevOf(from.id):null);
    const lift=prev.lastLift&&prev.lastLift.locId===from.id&&now-prev.lastLift.ts<LIFT_MEMORY_MS?prev.lastLift:null;
    pend={loc:from,ts:lift?lift.ts:now,lat:ac.lat,lon:ac.lon,refInd,inferred:prev.status==='nosignal'&&!lift};
    if(confirmDep(pend,ac,ind,now)){pushEvent(events,out,'DEPARTED',t,ac,pend.loc,pend.ts,pend.inferred,prev);return none;}
    return{dep:pend,arr:null};
  }
  if(prev.status==='airborne'&&status==='ground'){const td=touchdownLoc(near);if(td)return{dep:null,arr:{loc:td,ts:now,gs:ac.gs||0}};}
  return none;
}
function handleLost(t,prev,now,events,out){
  if(!prev||prev.status==='nosignal')return;
  if(now-prev.lastSeen<LOST_SEC*1000)return;
  const arr=prev.arrPend;prev.arrPend=null;
  if(arr&&(arr.gs||0)>TOUCHDOWN_CONFIRM_GS){prev.lostAirborne=true;prev.status='nosignal';prev.ts=now;return;} // that "touchdown" was a fast bad reading: treat as lost in flight
  if(arr){ // touched down, then went quiet: landed
    if(arr.replaces)(out.retract=out.retract||[]).push({hex:t.hex,ts:arr.replaces.ts,locId:arr.replaces.locId,why:'it landed at '+arr.loc.name+' instead'});
    pushEvent(arr.replaces?events.filter(e=>!(e.hex===t.hex&&e.type==='ARRIVED'&&e.ts===arr.replaces.ts)):events,out,'ARRIVED',t,prev,arr.loc,arr.ts,!!arr.inferred,prev);prev.status='nosignal';prev.ts=now;return;}
  const pend=prev.depPend;prev.depPend=null;
  if(pend){
    // went quiet without leaving the location it lifted from: no flight yet. Remember the liftoff in case it reappears en route.
    if(prev.nearest&&prev.nearest.id===pend.loc.id&&nearForEvent(prev.nearest)){prev.lastLift={locId:pend.loc.id,ts:pend.ts};prev.status='nosignal';prev.ts=now;return;}
    pushEvent(events,out,'DEPARTED',t,prev,pend.loc,pend.ts,pend.inferred,prev);
  }
  const cand=prev.status==='airborne'?landingCandidate(prev):null;
  const a=cand?(typeof prev.ind==='number'?aglAt(prev.ind,cand.id):(typeof prev.agl==='number'?prev.agl:prev.alt)):null;
  const looksLikeLanding=!((prev.gs||0)>LANDING_MAX_GS&&a!=null&&a>LANDING_FAST_MIN_AGL);
  const ceiling=cand?(cand.r<1||cand.aux?LOSS_MAX_AGL_PAD:LOSS_MAX_AGL_APT):0;
  if(cand&&a!=null&&a<=ceiling&&looksLikeLanding){
    pushEvent(events,out,'ARRIVED',t,prev,cand,now,true,prev);
    prev.lossArr={ts:now,locId:cand.id}; // kept so the arrival can be withdrawn or moved if the tail turns out otherwise
  }else if(prev.status==='airborne')prev.lostAirborne=true;
  prev.status='nosignal';prev.ts=now;
}
// One sample for one tail: classify, detect, and return the tail's new state.
function stepTail(t,ac,prev,now,events,out,wx){
  const status=ac?getStatus(ac):'stale';
  if(!ac||status==='stale'){handleLost(t,prev,now,events,out);return prev;}
  const near=snapNear(nearestLoc(ac.lat,ac.lon)),qnh=qnhFor(ac.lat,ac.lon,wx,now),ind=indAlt(ac,qnh),agl=near?aglAt(ind,near.id):null;
  const fs=flightStatus(ac,near,ind,qnh);
  const res=detectEvents(t,ac,prev,fs,ind,near,now,events,out,qnh),pend=res.dep;
  // Dwell tracking: which location the tail is (or was last) near, since when, and until when. Used to time reconstructed events.
  const nearEv=nearForEvent(near)?near:null,pn=prev||{};
  const dwell=nearEv?{nearLoc:nearEv,nearSince:(pn.nearLoc&&pn.nearLoc.id===nearEv.id&&pn.nearSince)?pn.nearSince:now,nearLast:now}
                    :{nearLoc:pn.nearLoc||null,nearSince:pn.nearSince||null,nearLast:pn.nearLast||null};
  // a remembered liftoff is used once, when the tail next shows up airborne; touching down anywhere clears it
  let lastLift=pn.lastLift&&now-pn.lastLift.ts<LIFT_MEMORY_MS?pn.lastLift:null;
  if(fs==='ground'||pend||(pn.status==='nosignal'&&fs==='airborne'))lastLift=null;
  return{...dwell,status:fs,alt:altOf(ac),ind,agl,gs:typeof ac.gs==='number'?ac.gs:null,depPend:pend,arrPend:res.arr,lastLift,alt_baro:ac.alt_baro,lat:ac.lat,lon:ac.lon,nearest:near,
    lastSeen:now-Math.round((ac.seen_pos!=null?ac.seen_pos:(ac.seen||0))*1000),ts:now,track:typeof ac.track==='number'?ac.track:null,
    r:ac.r||(prev&&prev.r)||'',t:ac.t||(prev&&prev.t)||'',desc:ac.desc||(prev&&prev.desc)||'',flight:(ac.flight||'').trim()};
}
function makeEvent(type,t,ac,loc,ts,inferred,synth){
  return{type,hex:t.hex,reg:regOf(t.hex,ac,t.label),label:t.label||'',locName:loc.name,locId:loc.id,dist:+(loc.dist||0).toFixed(2),
    callsign:(ac.flight||'').trim(),acType:ac.t||'',alt:typeof ac.alt_baro==='number'?Math.round(ac.alt_baro):(ac.alt_baro==='ground'?0:null),
    ts,inferred:!!inferred,synth:!!synth};
}
// Every leg should have both ends. If a tail arrives somewhere new without a departure on record (or departs a place it
// was never logged arriving at), the missing event is reconstructed from where the tail was last seen near a location.
function pushEvent(events,out,type,t,ac,loc,now,inferred,prev){
  const mine=[...out,...events].filter(e=>e.hex===t.hex).sort((a,b)=>b.ts-a.ts),last=mine[0];
  if(last&&last.type===type&&last.locId===loc.id)return; // same event at the same place again: a repeat, not a new leg
  if([...out,...events].find(e=>e.hex===t.hex&&e.type===type&&e.locId===loc.id&&Math.abs(now-(e.ts||0))<EVENT_GAP_SEC*1000))return; // a repeat is the same event at the same place
  if(last&&last.type==='ARRIVED'&&type==='DEPARTED'&&last.locId!==loc.id){
    // a whole flight went unseen: it left the last place it arrived at and landed here. Rebuild both ends (estimated).
    const dwell=prev||{},X=locById(last.locId),Y=locById(loc.id),fly=(X&&Y)?haversineNm(X.lat,X.lon,Y.lat,Y.lon)/110*3600e3+60e3:10*60e3;
    const arrTs=Math.min(now-1000,(dwell.nearLoc&&dwell.nearLoc.id===loc.id&&dwell.nearSince>last.ts)?dwell.nearSince:Math.max(last.ts+fly+60e3,now-60e3));
    const depTs=Math.max(last.ts+60e3,arrTs-fly);
    out.push(makeEvent('DEPARTED',t,ac,{id:last.locId,name:last.locName,dist:last.dist},Math.min(depTs,arrTs-1000),true,true));
    out.push(makeEvent('ARRIVED',t,ac,loc,arrTs,true,true));
  }
  if(last&&last.type===type){
    const dwell=prev||{};
    if(type==='ARRIVED'){ // it must have left the previous location: use the last moment it was seen near there
      const depTs=(dwell.nearLoc&&dwell.nearLoc.id===last.locId&&dwell.nearLast>last.ts)?dwell.nearLast:last.ts+60000;
      out.push(makeEvent('DEPARTED',t,ac,{id:last.locId,name:last.locName,dist:last.dist},Math.min(depTs,now-1000),true,true));
    }else{ // it must have arrived here first: use the moment it first came near this location
      const arrTs=(dwell.nearLoc&&dwell.nearLoc.id===loc.id&&dwell.nearSince>last.ts)?dwell.nearSince:Math.max(last.ts+60000,now-60000);
      out.push(makeEvent('ARRIVED',t,ac,loc,Math.min(arrTs,now-1000),true,true));
    }
  }
  out.push(makeEvent(type,t,ac,loc,now,inferred,false));
}

// ---- locations (locations.json on GitHub Pages, cached in KV, refreshed every 5 min) ----
async function getLocations(env){
  const now=Date.now(),cached=await env.KV.get('locations','json');
  if(cached&&cached.list&&cached.list.length&&now-(cached.fetchedAt||0)<LOCATIONS_EVERY_MS){LOCATIONS=cached.list;return LOCATIONS;}
  try{
    const r=await fetch(LOCATIONS_URL+'?ts='+now,{signal:AbortSignal.timeout(8000)});
    if(r.ok){const list=await r.json();
      const clean=(Array.isArray(list)?list:[]).filter(l=>l&&l.id&&typeof l.lat==='number'&&typeof l.lon==='number').map(l=>({id:l.id,name:l.name||l.id,lat:l.lat,lon:l.lon,r:l.r||2.0,elev:typeof l.elev==='number'?l.elev:undefined,aux:l.aux===true||undefined}));
      if(clean.length){LOCATIONS=clean;await env.KV.put('locations',JSON.stringify({list:clean,fetchedAt:now,source:'locations.json'}));return LOCATIONS;}}
  }catch(e){}
  if(cached&&cached.list&&cached.list.length){LOCATIONS=cached.list;await env.KV.put('locations',JSON.stringify({...cached,fetchedAt:now}));} // keep the last good copy, retry later
  else LOCATIONS=DEFAULT_LOCATIONS;
  return LOCATIONS;
}

async function getWaypoints(env){
  const now=Date.now(),cached=await env.KV.get('waypoints','json');
  if(cached&&cached.list&&now-(cached.fetchedAt||0)<LOCATIONS_EVERY_MS){WAYPOINTS=cached.list;return WAYPOINTS;}
  try{const r=await fetch(WAYPOINTS_URL+'?ts='+now,{signal:AbortSignal.timeout(8000)});
    if(r.ok){const list=await r.json();const clean=(Array.isArray(list)?list:[]).filter(w=>w&&w.name&&typeof w.lat==='number'&&typeof w.lon==='number'&&w.r>0);
      WAYPOINTS=clean;await env.KV.put('waypoints',JSON.stringify({list:clean,fetchedAt:now}));return WAYPOINTS;}}catch(e){}
  WAYPOINTS=(cached&&cached.list)||[];return WAYPOINTS;
}
// Position in words, for Slack: named area if inside one, else bearing/distance from the nearest station.
function nearestAny(lat,lon){let best=null,bd=Infinity;for(const L of LOCATIONS){const d=haversineNm(lat,lon,L.lat,L.lon);if(d<bd){bd=d;best=L;}}return best?{loc:best,dist:bd}:null;}
function bearingDeg(la1,lo1,la2,lo2){const r=Math.PI/180,y=Math.sin((lo2-lo1)*r)*Math.cos(la2*r),x=Math.cos(la1*r)*Math.sin(la2*r)-Math.sin(la1*r)*Math.cos(la2*r)*Math.cos((lo2-lo1)*r);return(Math.atan2(y,x)*180/Math.PI+360)%360;}
const COMPASS16=['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'],COMPASS8=['N','NE','E','SE','S','SW','W','NW'];
function compass(deg,pts){const names=pts===8?COMPASS8:COMPASS16,n=names.length;return names[Math.round((((deg%360)+360)%360)/(360/n))%n];}
function nearestWaypoint(lat,lon){let best=null,br=Infinity;for(const w of WAYPOINTS){const d=haversineNm(lat,lon,w.lat,w.lon);if(d<=w.r&&d/w.r<br){br=d/w.r;best=w;}}return best;}
function describePos(lat,lon,track,airborne){
  const st=nearestAny(lat,lon),wp=nearestWaypoint(lat,lon);
  const where=wp?((airborne?'over ':'near ')+wp.name):st?(st.dist.toFixed(0)+' nm '+compass(bearingDeg(st.loc.lat,st.loc.lon,lat,lon))+' of '+st.loc.name):(lat.toFixed(3)+', '+lon.toFixed(3));
  return{where,hdg:(airborne&&typeof track==='number')?compass(track,8):null,station:st,inArea:!!wp};
}

// ---- fleet list (KV, seeded from TAILS) ----
async function getTails(env){
  const t=await env.KV.get('tails','json');
  if(Array.isArray(t)&&t.length)return t;
  await env.KV.put('tails',JSON.stringify(TAILS));
  return TAILS.slice();
}
async function resolveHex(id){
  const s=(id||'').toUpperCase().replace(/[^A-Z0-9]/g,'');
  if(!s)return null;
  if(s[0]==='N'){const h=nToHex(s);if(h)return h;}
  else if(/^[0-9A-F]{6}$/.test(s))return s.toLowerCase();
  // non-US registration or unusual format: ask the feed
  try{const r=await upstream('https://api.adsb.lol/v2/reg/'+s,8000);if(r.ok){const j=await r.json();if(j.ac&&j.ac[0]&&j.ac[0].hex)return j.ac[0].hex.toLowerCase();}}catch(e){}
  return null;
}

// ---- Slack bot: @mention commands ----
async function handleSlack(req,env,ctx){
  const body=await req.text();
  if(!env.SLACK_SIGNING_SECRET)return new Response('signing secret not configured',{status:503});
  const ok=await verifySlack(env.SLACK_SIGNING_SECRET,req.headers.get('x-slack-request-timestamp'),body,req.headers.get('x-slack-signature'));
  if(!ok)return new Response('bad signature',{status:401});
  let payload;try{payload=JSON.parse(body);}catch(e){return new Response('bad json',{status:400});}
  if(payload.type==='url_verification')return new Response(JSON.stringify({challenge:payload.challenge}),{headers:{'Content-Type':'application/json'}});
  if(req.headers.get('x-slack-retry-num'))return new Response('ok'); // retry of an event already handled
  const ev=payload.type==='event_callback'?payload.event:null;
  if(ev&&(ev.type==='app_mention'||ev.type==='message')&&!ev.bot_id&&!ev.subtype&&ev.text)
    ctx.waitUntil(handleMessage(env,ev,ev.type==='app_mention'));
  return new Response('ok');
}
async function verifySlack(secret,ts,body,sig){
  if(!ts||!sig||!/^\d+$/.test(ts))return false;
  if(Math.abs(Date.now()/1000-Number(ts))>300)return false; // replay window
  const key=await crypto.subtle.importKey('raw',new TextEncoder().encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);
  const mac=await crypto.subtle.sign('HMAC',key,new TextEncoder().encode('v0:'+ts+':'+body));
  const expected='v0='+[...new Uint8Array(mac)].map(b=>b.toString(16).padStart(2,'0')).join('');
  if(expected.length!==sig.length)return false;
  let diff=0;for(let i=0;i<expected.length;i++)diff|=expected.charCodeAt(i)^sig.charCodeAt(i);
  return diff===0;
}
// Messages can arrive twice (Events API + channel polling), so every processed message ts is remembered.
async function alreadySeen(env,ts){
  const seen=(await env.KV.get('slack_seen','json'))||[];
  if(seen.includes(ts))return true;
  seen.push(ts);while(seen.length>200)seen.shift();
  await env.KV.put('slack_seen',JSON.stringify(seen));
  return false;
}
async function handleMessage(env,ev,mentioned){
  if(!ev.ts||await alreadySeen(env,ev.ts))return;
  const parsed=parseCommand(ev.text,mentioned);
  if(!parsed)return; // ordinary chatter in the channel: stay quiet
  let reply;
  try{reply=await runCommand(env,parsed);}catch(e){reply='Something went wrong: '+e.message;}
  await slackPost(env,ev.channel,reply,ev.thread_ts||ev.ts);
}
// Plain-language parsing: "add N84BL Robby's 407", "please remove N84BL", "where is N92N", "list", "status".
const KEYWORDS=[[/\b(recap|summary|report)\b/i,'recap'],[/\b(add|track|start tracking|watch|include)\b/i,'add'],[/\b(remove|delete|untrack|stop tracking|drop|exclude)\b/i,'remove'],
  [/\b(where|find|locate|position of)\b/i,'lookup'],[/\b(list|fleet|tails|tracking)\b/i,'list'],[/\b(status|health|alive|working)\b/i,'status'],[/\b(help|commands)\b/i,'help']];
function findTail(text){
  const m=/\b(N[1-9][0-9]{0,4}[A-Z]{0,2})\b/i.exec(text)||/\b([0-9A-F]{6})\b/i.exec(text);
  return m?{token:m[1].toUpperCase(),index:m.index,length:m[1].length}:null;
}
function countTails(text){return new Set([...text.matchAll(/\b(N[1-9][0-9]{0,4}[A-Z]{0,2})\b/gi)].map(m=>m[1].toUpperCase())).size;}
const LABEL_MAX=40;
// Several tails at once: the message must START with add/remove and contain nothing but tails (commas, "and", "from the tracker" allowed).
// "remove N62ZA, N76ZA and N88ZA" works; a sentence that merely mentions several tails still doesn't.
const MULTI_TAIL_MAX=25;
function parseMulti(text){
  const m=/^(?:please\s+)?(add|track|remove|delete|untrack|drop)\b[\s:,-]*(.*)$/i.exec(text);if(!m)return null;
  const rest=m[2].replace(/\b(?:(?:from|to|on|off)\s+(?:the\s+)?(?:tracker|fleet|list))\b/gi,' ').replace(/\b(and|please|thanks|thank you|pls)\b/gi,' ')
    .replace(/[,;.&!+\/]+/g,' ').trim().split(/\s+/).filter(Boolean);
  if(rest.length<2||rest.length>MULTI_TAIL_MAX)return null;
  if(!rest.every(w=>/^N[1-9][0-9]{0,4}[A-Z]{0,2}$/i.test(w)||/^(?=.*\d)[0-9A-F]{6}$/i.test(w)))return null;
  const tails=[...new Set(rest.map(w=>w.toUpperCase()))];if(tails.length<2)return null;
  return{cmd:/^(add|track)$/i.test(m[1])?'add':'remove',tail:tails[0],tails,label:'',text};
}
function parseCommand(rawText,mentioned){
  const text=(rawText||'').replace(/<@[^>]+>/g,' ').replace(/[‘’]/g,"'").replace(/\s+/g,' ').trim();
  const tail=findTail(text);
  const words=text.split(' ').filter(Boolean);
  // Guard rails so instructions, examples and chatter are never treated as commands:
  //  - quoted command examples ("add N123AB ...") or bullet lists are documentation, not requests
  //  - a message naming several different tails is ambiguous
  //  - a plain (un-mentioned) request must be short and start with the command word
  if(/["'`\u201c]\s*(add|remove|track|untrack|delete|where|list|status)\b/i.test(text)||/[\u2022]/.test(text))return null;
  const multi=parseMulti(text);if(multi)return multi;
  if(countTails(text)>1&&!mentioned)return null;
  if(!mentioned&&(words.length>12||!KEYWORDS.some(([re])=>re.test(words.slice(0,3).join(' ')))))return null;
  let cmd=null;for(const [re,name] of KEYWORDS){if(re.test(text)){cmd=name;break;}}
  if(cmd==='list'&&tail)cmd=null; // "who is N84BL" -> a lookup, not the fleet list
  if(!cmd&&tail)cmd='lookup';
  if(!cmd)return mentioned?{cmd:'help',text}:null;
  // Without a direct mention, a bare list/status/help only counts when the message is short and clearly aimed at the bot.
  if(!mentioned&&!tail&&(cmd==='list'||cmd==='status'||cmd==='help')&&text.split(' ').length>4)return null;
  if(cmd==='recap'){ // "recap for fleet", "recap N84BL yesterday", "recap for N84BL past 24 hours"
    const period=/\byesterday\b/i.test(text)?'yesterday':/\b(24 ?h(ou)?rs?|past day|last day|24hrs)\b/i.test(text)?'24h':'today';
    return{cmd,tail:tail?tail.token:null,period,label:'',text};
  }
  if((cmd==='add'||cmd==='remove'||cmd==='lookup')&&!tail)return mentioned?{cmd:'help',text}:null;
  if(cmd==='lookup'&&!mentioned&&!/^(where|find|locate|who|what|is)\b/i.test(text)&&!/^N[1-9]/i.test(text))return null; // "where is N92N" yes; a tail mentioned mid-sentence no
  let label='';
  if(cmd==='add'&&tail){
    label=text.slice(tail.index+tail.length).replace(/^[\s,.:;-]+/,'')
      .replace(/^(to|into|on|in)\s+(the\s+|our\s+)?(tracker|fleet|list|dashboard)\b[\s,.:;-]*/i,'')
      .replace(/^(as|called|named?|label(l?ed)?|with label|it'?s)\s+/i,'')
      .replace(/\b(please|thanks|thank you|pls)\b/gi,'').replace(/[\s,.!?"]+$/,'').replace(/^["']|["']$/g,'').trim();
    if(label.length>LABEL_MAX||/["\u2022]/.test(label)||countTails(label)>0)label=''; // not a label: drop it rather than store junk
  }
  return{cmd,tail:tail?tail.token:null,label,text};
}
function tailName(t){return hexToN(t.hex)||t.hex.toUpperCase();}
function describeTail(t,s){
  const shownAlt=typeof s?.ind==='number'?s.ind:s?.alt; // altimeter-corrected altitude (ft MSL) when known, not raw pressure altitude
  const st=!s?'no data yet':s.status==='airborne'?'airborne'+(shownAlt!=null?' at '+(Math.round(shownAlt/50)*50).toLocaleString()+' ft':''):s.status==='ground'?'on the ground':'not transmitting · last tracked '+fmtTime(s.lastSeen);
  let where='';
  if(s&&s.lat!=null){
    const n=s.nearest;
    if(n&&n.dist<=n.r)where=` · ${s.status==='airborne'?'over':'at'} ${n.name}`;
    else{const d=describePos(s.lat,s.lon,s.track,s.status==='airborne');
      where=` · ${d.where}`+(d.hdg?`, heading ${d.hdg}`:'')+(d.inArea&&d.station?` · ${d.station.dist.toFixed(0)} nm from ${d.station.loc.id}`:'');}
  }
  return `*${tailName(t)}*${t.label?' ('+t.label+')':''} — ${st}${where}`;
}
async function runCommand(env,p){
  const tails=await getTails(env);await getLocations(env);await getWaypoints(env);
  if(p.tails&&(p.cmd==='add'||p.cmd==='remove')){ // several tails in one message: one KV write, one summary reply
    const done=[],already=[],bad=[];
    for(const tok of p.tails){
      const hex=await resolveHex(tok);if(!hex){bad.push(tok);continue;}
      const i=tails.findIndex(t=>t.hex===hex);
      if(p.cmd==='add'){if(i>=0){already.push(tailName(tails[i]));continue;}tails.push({hex,label:''});done.push(hexToN(hex)||hex.toUpperCase());}
      else{if(i<0){already.push(tok);continue;}const [t]=tails.splice(i,1);done.push(tailName(t));}
    }
    if(done.length)await env.KV.put('tails',JSON.stringify(tails));
    const b=x=>x.map(n=>`*${n}*`).join(', '),out=[];
    if(done.length)out.push(p.cmd==='add'?`:white_check_mark: Added ${b(done)}. They show on the dashboard within a minute.`:`:wastebasket: Removed ${b(done)}.`);
    if(already.length)out.push(p.cmd==='add'?`Already on the tracker: ${b(already)}.`:`Not on the tracker: ${b(already)}.`);
    if(bad.length)out.push(`Couldn't match ${b(bad)} to an aircraft (use N-numbers like N84BL or 6-character ICAO hex).`);
    out.push(`Now tracking ${tails.length} tails.`);
    return out.join('\n');
  }
  if(p.cmd==='add'){
    const hex=await resolveHex(p.tail);
    if(!hex)return `I couldn't match *${p.tail}* to an aircraft. Use a US N-number like N84BL or a 6-character ICAO hex.`;
    const existing=tails.find(t=>t.hex===hex);
    if(existing){if(p.label&&p.label!==existing.label){existing.label=p.label;await env.KV.put('tails',JSON.stringify(tails));return `*${tailName(existing)}* was already on the tracker. Label updated to "${p.label}".`;}return `*${tailName(existing)}* is already on the tracker.`;}
    tails.push({hex,label:p.label});await env.KV.put('tails',JSON.stringify(tails));
    return `:white_check_mark: Added *${hexToN(hex)||hex.toUpperCase()}*${p.label?' ('+p.label+')':''} (hex ${hex.toUpperCase()}). Now tracking ${tails.length} tails. It shows on the dashboard within a minute.`;
  }
  if(p.cmd==='remove'){
    const hex=await resolveHex(p.tail),i=hex?tails.findIndex(t=>t.hex===hex):-1;
    if(i<0)return `*${p.tail}* isn't on the tracker.`;
    const [t]=tails.splice(i,1);await env.KV.put('tails',JSON.stringify(tails));
    return `:wastebasket: Removed *${tailName(t)}*. Now tracking ${tails.length} tails.`;
  }
  if(p.cmd==='lookup'){
    const hex=await resolveHex(p.tail),t=hex?tails.find(x=>x.hex===hex):null;
    if(!t)return `*${p.tail}* isn't on the tracker. Say "add ${p.tail}" to start tracking it.`;
    const state=(await env.KV.get('state','json'))||{};
    return describeTail(t,state[t.hex]);
  }
  if(p.cmd==='list'){
    const state=(await env.KV.get('state','json'))||{};
    return `*Tracking ${tails.length} tails*\n`+tails.map(t=>'• '+describeTail(t,state[t.hex])).join('\n');
  }
  if(p.cmd==='recap')return recapText(env,p,tails);
  if(p.cmd==='status'){
    const h=(await env.KV.get('health','json'))||{};
    return `Last sweep ${h.lastRun?fmtTime(h.lastRun):'never'} · ${h.ok?'feeds ok':'feeds unavailable'} · ${h.live??0} tails live · ${h.events??0} events logged · Slack posting ${h.slackOk===false?'failed':'ok'}`;
  }
  return 'I track the BLADE fleet and post departures and arrivals here. Just tell me in plain words:\n• "add N84BL Robby\'s 407" — start tracking a tail (label optional)\n• "remove N84BL" — stop tracking\n• "where is N84BL" — one tail\'s status\n• "list" — every tail\n• "recap for fleet" / "recap N84BL yesterday" — flights, top route and average ETE (today, yesterday or past 24 hours)\n• "status" — tracker health';
}
// Channel polling: independent of Slack's event delivery. Reads new messages in the tracker channel each sweep.
async function slackBackingOff(env){const u=+(await env.KV.get('slack_backoff_until')||0);return Date.now()<u;}
async function pollSlackChannel(env){return pollChannel(env,env.SLACK_CHANNEL_ID,'slack_cursor',(ev)=>handleMessage(env,ev,false));}
async function pollChannel(env,channelId,cursorKey,handler){
  if(!env.SLACK_BOT_TOKEN||!channelId)return;
  const cursor=await env.KV.get(cursorKey);
  const params=new URLSearchParams({channel:channelId,limit:'20'});
  if(cursor)params.set('oldest',cursor);
  const r=await fetch('https://slack.com/api/conversations.history?'+params,{headers:{'Authorization':'Bearer '+env.SLACK_BOT_TOKEN}});
  const j=await r.json();
  if(!j.ok){await env.KV.put('slack_poll_error',j.error||'unknown');if(j.error==='ratelimited')await env.KV.put('slack_backoff_until',String(Date.now()+1000*Math.max(30,+(r.headers.get('retry-after')||30))));return;}
  await env.KV.delete('slack_poll_error');
  const msgs=(j.messages||[]).filter(m=>m.type==='message'&&!m.bot_id&&!m.subtype&&m.text&&m.ts!==cursor).sort((a,b)=>+a.ts-+b.ts);
  if(!cursor){ // first run: only mark the position, never replay old history
    const latest=(j.messages||[]).reduce((mx,m)=>+m.ts>+mx?m.ts:mx,'0');
    await env.KV.put(cursorKey,latest===  '0'?String(Date.now()/1000):latest);return;
  }
  let last=cursor;
  for(const m of msgs){await handler({...m,channel:channelId});if(+m.ts>+last)last=m.ts;}
  const newest=(j.messages||[]).reduce((mx,m)=>+m.ts>+mx?m.ts:mx,last);
  if(newest!==cursor)await env.KV.put(cursorKey,newest);
}
async function slackPost(env,channel,text,thread_ts){
  if(!env.SLACK_BOT_TOKEN)return;
  await fetch('https://slack.com/api/chat.postMessage',{method:'POST',headers:{'Content-Type':'application/json; charset=utf-8','Authorization':'Bearer '+env.SLACK_BOT_TOKEN},
    body:JSON.stringify({channel,text,thread_ts,unfurl_links:false})});
}

// ===== Flight-category watch (second channel). Aviation Weather Center METAR API; notifies on VFR/MVFR/IFR/LIFR changes. =====
const WX_API='https://aviationweather.gov/api/data/metar',WX_EVERY_MS=5*60e3,WX_STALE_MS=2*3600e3;
const DEFAULT_STATIONS=['KLGA','KEWR','KTEB','KJFK','KCDW','KMMU','KHPN','KFRG','KISP','KHWV','KFOK','KJPX','KMTP','KSWF','KPOU','KMSV','KBLM','KMJX','KACY'];
const CAT_RANK={LIFR:0,IFR:1,MVFR:2,VFR:3},CAT_EMOJI={VFR:':large_green_circle:',MVFR:':large_blue_circle:',IFR:':red_circle:',LIFR:':large_purple_circle:'};
async function getStations(env){let l=await env.KV.get('wx_stations','json');if(!Array.isArray(l)||!l.length){l=DEFAULT_STATIONS.slice();await env.KV.put('wx_stations',JSON.stringify(l));}return l;}
async function fetchMetars(ids){
  const url=WX_API+'?format=json&ids='+ids.join(',');
  try{const r=await fetch(url,{headers:{'Accept':'application/json','User-Agent':'blade-fleet-events/1.0'},signal:AbortSignal.timeout(12000)});if(r.ok){const j=await r.json();if(Array.isArray(j))return j;}}catch(e){}
  try{const r=await upstream(url,15000);if(r.ok){const j=await r.json();if(Array.isArray(j))return j;}}catch(e){} // via the Vercel proxy if Cloudflare egress is refused
  return null;
}
function wxName(m,id){return m&&m.name?m.name.replace(/,.*$/,'').replace(/\b(Arpt|Intl|Muni|Rgnl|Cnty)\b/g,'').replace(/\s+/g,' ').trim():id;}
function wxSummary(m){
  const parts=[];
  if(m.visib!=null)parts.push('vis '+String(m.visib).replace('+','')+' SM');
  const ceil=(m.clouds||[]).filter(c=>['BKN','OVC','VV'].includes(c.cover)&&c.base!=null).map(c=>+c.base).sort((a,b)=>a-b)[0];
  parts.push(ceil!=null?'ceiling '+ceil.toLocaleString()+' ft':'no ceiling');
  if(m.wxString)parts.push(m.wxString);
  return parts.join(' · ');
}
function latestByStation(metars){const by={};for(const m of metars){const t=Date.parse(m.reportTime||m.obsTime);if(!isNaN(t)&&(!by[m.icaoId]||t>by[m.icaoId]._t)){m._t=t;by[m.icaoId]=m;}}return by;}
async function wxSweep(env){
  if(!env.WX_CHANNEL_ID)return;
  const now=Date.now(),st=(await env.KV.get('wx_state','json'))||{last:0,stations:{}};
  if(now-(st.last||0)<WX_EVERY_MS)return;
  const ids=await getStations(env),metars=await fetchMetars(ids);
  if(!metars){st.last=now;st.error='api unavailable';await env.KV.put('wx_state',JSON.stringify(st));return;}
  const by=latestByStation(metars),lines=[];
  for(const id of ids){
    const m=by[id],prev=st.stations[id]||{};
    if(!m||now-m._t>WX_STALE_MS||!m.fltCat){st.stations[id]={...prev,stale:true,...(m&&typeof m.altim==='number'&&now-m._t<=WX_STALE_MS?{altim:+(m.altim*0.02953).toFixed(2),lat:m.lat,lon:m.lon,obs:m._t}:{})};continue;}
    const cat=m.fltCat;
    if(prev.cat&&prev.cat!==cat){
      const dir=CAT_RANK[cat]>CAT_RANK[prev.cat]?'improving':'deteriorating';
      lines.push(wxChangeLine(id,m,prev,cat,dir));
    }
    st.stations[id]={cat,since:(prev.cat===cat&&prev.since)?prev.since:m._t,obs:m._t,stale:false,name:wxName(m,id),summary:wxSummary(m),altim:typeof m.altim==='number'?+(m.altim*0.02953).toFixed(2):null,lat:m.lat,lon:m.lon};
  }
  st.last=now;delete st.error;await env.KV.put('wx_state',JSON.stringify(st));
  for(const l of lines)await slackSendTo(env,env.WX_CHANNEL_ID,l);
}
// House rules: station-specific wording when a station deteriorates (and only then). `mention` is a Slack user ID.
const WX_HOUSE_RULES={
  KMPO:{down:'WE HAVE LOST MOUNT POCONO'},
  KACK:{down:'What a horrible place to be'},
};
function wxChangeLine(id,m,prev,cat,dir){
  const detail=`${prev.cat} → *${cat}* (${dir}) · ${wxSummary(m)} · was ${prev.cat} since ${fmtTime(prev.since||prev.obs)} · obs ${fmtTime(m._t)}`;
  const rule=WX_HOUSE_RULES[id];
  if(rule&&dir==='deteriorating')return `:rotating_light: *${rule.down}*${rule.mention?' <@'+rule.mention+'>':''} — *${id}* ${wxName(m,id)}: ${detail}`;
  return `${CAT_EMOJI[cat]||''} *${id}* ${wxName(m,id)}: ${detail}`;
}
function wxStatusText(st,ids){
  const rows=ids.map(id=>({id,...(st.stations[id]||{})})).sort((a,b)=>(CAT_RANK[a.cat]??9)-(CAT_RANK[b.cat]??9)||a.id.localeCompare(b.id));
  return `*Flight categories — ${ids.length} stations*\n`+rows.map(r=>r.cat?`${CAT_EMOJI[r.cat]} *${r.id}* ${r.name||''} — ${r.cat}${r.stale?' (stale)':''} since ${fmtTime(r.since)} · ${r.summary||''}`:`:white_circle: *${r.id}* — no recent report`).join('\n');
}
// Commands in the weather channel: "add KHVN", "remove KISP", "list" / "now", "help"
async function wxHandleMessage(env,ev){
  if(!ev.ts||await alreadySeen(env,ev.ts))return;
  const text=(ev.text||'').replace(/<@[^>]+>/g,' ').replace(/\s+/g,' ').trim(),words=text.split(' ').filter(Boolean);
  if(!words.length||words.length>6)return;
  const c=words[0].toLowerCase(),arg=(words[1]||'').toUpperCase().replace(/[^A-Z0-9]/g,'');
  const norm=id=>id.length===3?'K'+id:id;
  let reply=null;const ids=await getStations(env);
  if((c==='add'||c==='watch')&&arg){
    const id=norm(arg);
    if(ids.includes(id))reply=`*${id}* is already watched.`;
    else{const m=await fetchMetars([id]);const latest=m&&latestByStation(m)[id];
      if(!latest)reply=`No METAR-reporting station found for *${id}*. Use the ICAO ID (KHVN, KBDR, …).`;
      else{ids.push(id);await env.KV.put('wx_stations',JSON.stringify(ids));
        const st=(await env.KV.get('wx_state','json'))||{last:0,stations:{}};st.stations[id]={cat:latest.fltCat||null,since:latest._t,obs:latest._t,stale:false,name:wxName(latest,id),summary:wxSummary(latest),altim:typeof latest.altim==='number'?+(latest.altim*0.02953).toFixed(2):null,lat:latest.lat,lon:latest.lon};await env.KV.put('wx_state',JSON.stringify(st));
        reply=`:white_check_mark: Watching *${id}* ${wxName(latest,id)} — currently ${latest.fltCat||'unknown'} · ${wxSummary(latest)}. ${ids.length} stations.`;}}
  }else if((c==='remove'||c==='delete'||c==='drop')&&arg){
    const id=norm(arg),i=ids.indexOf(id);
    if(i<0)reply=`*${id}* isn't on the list.`;else{ids.splice(i,1);await env.KV.put('wx_stations',JSON.stringify(ids));reply=`:wastebasket: Stopped watching *${id}*. ${ids.length} stations.`;}
  }else if(c==='list'||c==='now'||c==='status'||c==='stations'||c==='current'){
    reply=wxStatusText((await env.KV.get('wx_state','json'))||{stations:{}},ids);
  }else if(c==='help'||c==='commands'){
    reply='I post when a station changes flight category (VFR / MVFR / IFR / LIFR), checking every 5 minutes.\n• "add KHVN" — watch a station\n• "remove KISP" — stop watching\n• "list" — every station and its current category';
  }
  if(reply)await slackSendTo(env,ev.channel,reply,ev.thread_ts||ev.ts);
}

// ---- one-time record corrections (each runs once, guarded by a KV flag) ----
async function oneTimeFixes(env,events){
  await oneTimeFix5(env,events);
  await oneTimeFix4(env,events);
  await oneTimeFix2(env,events);
  await oneTimeFix3(env);
  const FLAG='fix_2026-10-30_N76ZA';
  if(await env.KV.get(FLAG))return;
  const hex='aa416f',find=(type,loc,ts)=>events.find(e=>e.hex===hex&&e.type===type&&e.locId===loc&&e.ts===ts);
  const lgaA=find('ARRIVED','LGA',1793389760421),lgaD=find('DEPARTED','LGA',1793389784419),fokA=find('ARRIVED','FOK',1793377003255),fokD=find('DEPARTED','FOK',1793377063255);
  const bdrD=find('DEPARTED','BDR',1793388702968),w30D=find('DEPARTED','W30',1793372893834),htoA=find('ARRIVED','HTO',1793377640555);
  for(const e of [lgaA,lgaD,fokA,fokD]){if(e)events.splice(events.indexOf(e),1);}
  let e34=events.find(e=>e.hex===hex&&e.type==='ARRIVED'&&e.locId==='E34'&&Math.abs(e.ts-1793389820910)<15*60e3);
  if(!e34&&bdrD){const L=locById('E34');e34={type:'ARRIVED',hex,reg:'N76ZA',label:'',locName:L?L.name:'East 34th St Heliport',locId:'E34',dist:0.26,callsign:'N76ZA',acType:'S76',alt:0,ts:1793389820910,inferred:true,synth:false};
    const k=events.findIndex(e=>e.ts<e34.ts);events.splice(k<0?events.length:k,0,e34);}
  const legs=await getLegs(env);
  const drop=l=>l.hex===hex&&[1793389760421,1793377003255,1793377640555].includes(l.arr);
  for(let i=legs.length-1;i>=0;i--)if(drop(legs[i]))legs.splice(i,1);
  const add=[];if(w30D&&htoA&&!legs.find(l=>l.hex===hex&&l.arr===htoA.ts&&l.from==='W30'))add.push(legRecord(w30D,htoA));if(bdrD&&e34&&!legs.find(l=>l.hex===hex&&l.arr===e34.ts))add.push(legRecord(bdrD,e34));
  for(const l of add){const k=legs.findIndex(x=>x.arr<l.arr);legs.splice(k<0?legs.length:k,0,l);}
  await env.KV.put('legs',JSON.stringify(legs));
  // strike the wrong Slack posts (found by text in the channel around the time they went out)
  try{
    const r=await fetch('https://slack.com/api/conversations.history?'+new URLSearchParams({channel:env.SLACK_CHANNEL_ID,oldest:String(1793376400),latest:String(1793390400),limit:'200'}),{headers:{'Authorization':'Bearer '+env.SLACK_BOT_TOKEN}});
    const j=await r.json();
    for(const m of (j.messages||[])){
      const tx=m.text||'';if(!/\*N76ZA\*/.test(tx)||/^~/.test(tx))continue;
      if(/arrived at \*LaGuardia/.test(tx))await slackEdit(env,m.ts,'~*N76ZA* arrived at *LaGuardia (LGA)* — 3:49 PM~ · _corrected: one bad "on ground" reading while passing LaGuardia; it landed at East 34th St Heliport at 3:50 PM_');
      else if(/departed \*LaGuardia/.test(tx))await slackEdit(env,m.ts,'~*N76ZA* departed *LaGuardia (LGA)* — 3:49 PM~ · _corrected: it never landed at LaGuardia_');
      else if(/Westhampton Gabreski/.test(tx))await slackEdit(env,m.ts,'~'+tx.replace(/~/g,'')+'~ · _corrected: signal dropped while passing Westhampton; no stop there_');
    }
  }catch(e){}
  await env.KV.put(FLAG,new Date().toISOString());
}

// Second pass for N76ZA 2026-10-30: the rebuilt East 34th arrival had produced a 1-min LGA->E34 leg; replace it with BDR->E34,
// re-try any Slack post that still isn't struck through, and record what was done (readable at /fixlog).
async function oneTimeFix2(env,events){
  const FLAG='fix_2026-10-30_N76ZA_v2';
  if(await env.KV.get(FLAG))return;
  const hex='aa416f',rep={at:new Date().toISOString(),legsRemoved:0,legAdded:false,slackFound:0,slackStruck:0,slackEdited:0,slackError:null};
  const legs=await getLegs(env);
  for(let i=legs.length-1;i>=0;i--){const l=legs[i];if(l.hex===hex&&(l.from==='LGA'||l.to==='LGA'||l.from==='FOK'||l.to==='FOK')&&l.arr>1793370000000){legs.splice(i,1);rep.legsRemoved++;}}
  const bdrD=events.find(e=>e.hex===hex&&e.type==='DEPARTED'&&e.locId==='BDR'&&e.ts===1793388702968);
  const e34=events.find(e=>e.hex===hex&&e.type==='ARRIVED'&&e.locId==='E34'&&Math.abs(e.ts-1793389820910)<15*60e3);
  if(bdrD&&e34&&!legs.find(l=>l.hex===hex&&l.arr===e34.ts)){const l=legRecord(bdrD,e34);const k=legs.findIndex(x=>x.arr<l.arr);legs.splice(k<0?legs.length:k,0,l);rep.legAdded=true;}
  await env.KV.put('legs',JSON.stringify(legs));
  try{
    const r=await fetch('https://slack.com/api/conversations.history?'+new URLSearchParams({channel:env.SLACK_CHANNEL_ID,oldest:String(1793376400),latest:String(1793390400),limit:'200'}),{headers:{'Authorization':'Bearer '+env.SLACK_BOT_TOKEN}});
    const j=await r.json();if(!j.ok)rep.slackError=j.error||'history failed';
    for(const m of (j.messages||[])){
      const tx=m.text||'';if(!/\*N76ZA\*/.test(tx)||!/LaGuardia|Westhampton/.test(tx))continue;
      rep.slackFound++;if(/^~/.test(tx)){rep.slackStruck++;continue;}
      const why=/LaGuardia/.test(tx)?(/arrived/.test(tx)?'one bad "on ground" reading while passing LaGuardia; it landed at East 34th St Heliport at 3:50 PM':'it never landed at LaGuardia'):'signal dropped while passing Westhampton; no stop there';
      if(await slackEdit(env,m.ts,'~'+tx.replace(/~/g,'')+'~ · _corrected: '+why+'_')){rep.slackEdited++;rep.slackStruck++;}
    }
  }catch(e){rep.slackError=String(e.message||e);}
  await env.KV.put(FLAG,JSON.stringify(rep));
}

// Third pass: the two real N76ZA arrivals whose "from" was wrong were struck by mistake. Restore them with the right origin.
async function oneTimeFix3(env){
  const FLAG='fix_2026-10-30_N76ZA_v3';
  if(await env.KV.get(FLAG))return;
  const rep={at:new Date().toISOString(),restored:[],slackError:null};
  const fixed={
    'East Hampton':':large_green_circle: *N76ZA* arrived at *East Hampton (HTO)* — 12:27 PM from *West 30th St Heliport* · 1 h 19 min en route · _corrected: origin was West 30th, not Westhampton_',
    'East 34th':':large_green_circle: *N76ZA* arrived at *East 34th St Heliport* — 3:50 PM from *Bridgeport Sikorsky (BDR)* · 19 min en route · _corrected: origin was Bridgeport, not LaGuardia; time estimated_'};
  try{
    const r=await fetch('https://slack.com/api/conversations.history?'+new URLSearchParams({channel:env.SLACK_CHANNEL_ID,oldest:String(1793376400),latest:String(1793390400),limit:'200'}),{headers:{'Authorization':'Bearer '+env.SLACK_BOT_TOKEN}});
    const j=await r.json();if(!j.ok)rep.slackError=j.error||'history failed';
    for(const m of (j.messages||[])){
      const tx=m.text||'';if(!/\*N76ZA\*/.test(tx)||!/^~/.test(tx)||!/arrived at/.test(tx))continue;
      for(const k of Object.keys(fixed))if(tx.includes('arrived at *'+k)&&await slackEdit(env,m.ts,fixed[k]))rep.restored.push(k);
    }
  }catch(e){rep.slackError=String(e.message||e);}
  await env.KV.put(FLAG,JSON.stringify(rep));
}

// 2026-11-05: two estimated arrivals were wrongly withdrawn as pass-bys (quick stops). Restore them in the leg history,
// the event log (N36HF only; N88ZA's day has rolled out of the log) and Slack. Runs once; report at /fixlog.
async function oneTimeFix4(env,events){
  const FLAG='fix_2026-11-05_withdrawals';if(await env.KV.get(FLAG))return;
  const rep={at:new Date().toISOString(),legsSplit:0,eventsAdded:0,slackRestored:0,errors:[]};
  const loc=id=>{const L=locById(id);return{locId:id,locName:L?L.name:id};};
  const ev=(type,hex,reg,id,ts)=>({type,hex,reg,label:'',...loc(id),dist:0,callsign:reg,acType:'',alt:null,ts,inferred:true,synth:false});
  const legs=await getLegs(env);
  const split=(hex,reg,from,to,depTs,arrTs,via,viaArr,viaDep)=>{
    const i=legs.findIndex(l=>l.hex===hex&&l.from===from&&l.to===to&&l.arr===arrTs);if(i<0){rep.errors.push('leg not found '+reg);return;}
    const d=legs[i].dep;legs.splice(i,1,legRecord(ev('DEPARTED',hex,reg,via,viaDep),ev('ARRIVED',hex,reg,to,arrTs)),legRecord(ev('DEPARTED',hex,reg,from,d),ev('ARRIVED',hex,reg,via,viaArr)));rep.legsSplit++;
  };
  split('ac1dbc','N88ZA','E34','E34',1793396446668,1793397500434,'JFK',1793396820000,1793397060000);
  const n36=legs.find(l=>l.reg==='N36HF'&&l.from==='N12'&&l.to==='29NY'&&l.arr===1793573863720);
  if(n36){split(n36.hex,'N36HF','N12','29NY',n36.dep,1793573863720,'PHL',1793571180000,1793571360000);
    for(const e of [ev('ARRIVED',n36.hex,'N36HF','PHL',1793571180000),ev('DEPARTED',n36.hex,'N36HF','PHL',1793571360000)]){
      if(events.find(x=>x.hex===e.hex&&x.type===e.type&&x.locId==='PHL'&&Math.abs(x.ts-e.ts)<10*60e3))continue;
      const k=events.findIndex(x=>x.ts<e.ts);events.splice(k<0?events.length:k,0,e);rep.eventsAdded++;}
  }else rep.errors.push('N36HF leg not found');
  await env.KV.put('legs',JSON.stringify(legs));
  const fixes=[['1793396864.428829',':large_green_circle: *N88ZA* arrived at *JFK International* — 5:47 PM from *East 34th St Heliport* · 6 min en route · _estimated · restored: it did land (quick turn, back out about 5:51 PM)_'],
               ['1793571208.225719',':large_green_circle: *N36HF* arrived at *Philadelphia (PHL)* — 5:13 PM from *Lakewood Airport (N12)* · 23 min en route · _estimated · restored: it most likely landed (back out about 5:16 PM)_']];
  for(const [ts,text] of fixes){if(await slackEdit(env,ts,text))rep.slackRestored++;else rep.errors.push('slack edit failed '+ts);}
  await env.KV.put(FLAG,JSON.stringify(rep));
}

// 2026-11-17: N92N stopped at Trenton before TTN was on the location list. Add the stop (estimated) to the log, the leg
// history and Slack. Runs once; report at /fixlog.
async function oneTimeFix5(env,events){
  const FLAG='fix_2026-11-17_N92N_TTN';if(await env.KV.get(FLAG))return;
  const hex='acbcfd',reg='N92N',ARR=1794941460000,DEP=1794943075000,rep={at:new Date().toISOString(),added:0,leg:false,slack:0,errors:[]};
  const L=locById('TTN');if(!L){rep.errors.push('TTN not in location list yet');await env.KV.put(FLAG+'_wait',rep.at);return;} // try again next sweep
  const mk=(type,ts)=>({type,hex,reg,label:'',locId:'TTN',locName:L.name,dist:0,callsign:reg,acType:'',alt:null,ts,inferred:true,synth:false});
  const a=mk('ARRIVED',ARR),d=mk('DEPARTED',DEP);
  for(const e of [a,d]){if(events.find(x=>x.hex===hex&&x.type===e.type&&x.locId==='TTN'&&Math.abs(x.ts-e.ts)<30*60e3))continue;const k=events.findIndex(x=>x.ts<e.ts);events.splice(k<0?events.length:k,0,e);rep.added++;}
  const dep12=events.filter(x=>x.hex===hex&&x.type==='DEPARTED'&&x.ts<ARR).sort((x,y)=>y.ts-x.ts)[0];
  const legs=await getLegs(env);
  if(dep12&&!legs.find(l=>l.hex===hex&&l.arr===ARR)){const l=legRecord(dep12,a);const k=legs.findIndex(x=>x.arr<l.arr);legs.splice(k<0?legs.length:k,0,l);await env.KV.put('legs',JSON.stringify(legs));rep.leg=true;}
  const from=dep12?` from *${dep12.locName}* · ${fmtDur(ARR-dep12.ts)} en route`:'';
  for(const text of [`:large_green_circle: *${reg}* arrived at *${L.name}* — ${fmtTime(ARR)}${from} · _estimated · added late: Trenton was not on the location list yet_`,
                     `:small_red_triangle: *${reg}* departed *${L.name}* — ${fmtTime(DEP)} · _estimated · added late: Trenton was not on the location list yet_`])
    if(await slackSend(env,text))rep.slack++;else rep.errors.push('slack post failed');
  await env.KV.put(FLAG,JSON.stringify(rep));
}

// ---- data ----
async function fetchFeed(tails){
  const batch=tails.map(t=>t.hex).join(',').toUpperCase();
  for(const base of FEEDS){
    try{const r=await upstream(base+batch,12000);
      if(!r.ok)continue;const j=await r.json();if(j&&Array.isArray(j.ac))return j.ac;}catch(e){}
  }
  return null;
}
async function backfillTraces(state,now,tails){
  // Only for tails the tracker has never seen. A tail it already knows keeps its own state (incl. 'went quiet in flight'),
  // which a trace overwrite would erase.
  const silent=tails.filter(t=>!state[t.hex]);
  await Promise.all(silent.map(async t=>{
    try{
      const r=await upstream(TRACE_BASE+t.hex.slice(-2)+'/trace_recent_'+t.hex+'.json',12000);
      if(!r.ok)return;const d=await r.json();const tr=d.trace||[];if(!tr.length)return;
      const last=tr[tr.length-1],ts=Math.round((d.timestamp+last[0])*1000),p=state[t.hex];
      if(state[t.hex])return;
      const lat=last[1],lon=last[2],alt=last[3]==='ground'?0:(typeof last[3]==='number'?last[3]:null);
      state[t.hex]={status:'nosignal',alt,alt_baro:last[3],lat,lon,nearest:snapNear(nearestLoc(lat,lon)),lastSeen:ts,ts:now,
        r:d.r||(p&&p.r)||'',t:d.t||(p&&p.t)||'',desc:d.desc||(p&&p.desc)||'',flight:'',fromTrace:true};
    }catch(e){}
  }));
}

async function sweep(env,pollSlack=true){
  const now=Date.now();
  const state=(await env.KV.get('state','json'))||{};
  const events=(await env.KV.get('events','json'))||[];
  const health=(await env.KV.get('health','json'))||{};
  const tails=await getTails(env);
  await getLocations(env);await getWaypoints(env);
  for(const k in state){const p=state[k];if(p&&p.lat!=null)p.nearest=snapNear(nearestLoc(p.lat,p.lon));} // re-evaluate stored positions against the current location table
  const fresh=await fetchFeed(tails);
  if(!fresh){await env.KV.put('health',JSON.stringify({...health,lastRun:now,ok:false,error:'feeds unavailable'}));return;}
  const byHex={};for(const a of fresh)byHex[a.hex]=a;
  const out=[];
  const wx=(await env.KV.get('wx_state','json'))||null;
  for(const t of tails){const ns=stepTail(t,byHex[t.hex],state[t.hex],now,events,out,wx);if(ns)state[t.hex]=ns;}
  await applyRetractions(env,events,out.retract);
  await oneTimeFixes(env,events);
  let lastTrace=health.lastTrace||0;
  if(now-lastTrace>TRACE_EVERY_MS){await backfillTraces(state,now,tails);lastTrace=now;}
  let slackOk=health.slackOk;
  if(out.length){
    events.unshift(...out.slice().reverse());
    events.length=Math.min(events.length,MAX_EVENTS);
    const legs=await getLegs(env);let added=false;
    for(const e of out.filter(x=>x.type==='ARRIVED').sort((a,b)=>a.ts-b.ts)){const lg=legFor(e,events);if(lg&&!legs.find(x=>x.hex===e.hex&&x.arr===e.ts)){legs.unshift(legRecord(lg.from,lg.to));added=true;}}
    if(added){legs.length=Math.min(legs.length,MAX_LEGS);await env.KV.put('legs',JSON.stringify(legs));}
    slackOk=await postSlack(env,out,events);
  }
  await env.KV.put('state',JSON.stringify(state));
  await env.KV.put('events',JSON.stringify(events));
  await env.KV.put('health',JSON.stringify({lastRun:now,ok:true,live:fresh.length,lastTrace,slackOk,events:events.length,slackPollError:(await env.KV.get('slack_poll_error'))||null}));
  // Slack reads are shared with five other BLADE bots on the same app: read each channel once a minute, not every sample.
  if(pollSlack&&!(await slackBackingOff(env))){try{await pollSlackChannel(env);}catch(e){await env.KV.put('slack_poll_error',String(e.message||e));}}
  try{await wxSweep(env);}catch(e){console.error('wx sweep',e&&e.message);}
  if(pollSlack&&!(await slackBackingOff(env))){try{await pollChannel(env,env.WX_CHANNEL_ID,'wx_cursor',(ev)=>wxHandleMessage(env,ev));}catch(e){console.error('wx poll',e&&e.message);}}
}

// ---- Slack ----
function fmtTime(ts){return new Date(ts).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZone:TZ});}
// One line per event. Arrivals carry the origin and time en route inline (no thread) when the leg is known.
function slackLine(e,leg){
  const arrow=e.type==='DEPARTED'?':small_red_triangle:':':large_green_circle:'; // red up-triangle = departed, green circle = arrived
  const verb=e.type==='DEPARTED'?'departed':'arrived at';
  const note=e.route?' · _estimated from route: not tracked near there_':e.synth?' · _estimated (leg reconstructed)_':e.inferred?' · _estimated from last tracked position_':'';
  const cs=e.callsign&&e.callsign!==e.reg?' ('+e.callsign+')':'';
  const from=(e.type==='ARRIVED'&&leg)?` from *${leg.from.locName}* · ${fmtDur(leg.ete)} en route`+(leg.from.inferred&&!e.inferred?' _(departure time estimated)_':''):'';
  return `${arrow} *${e.reg}*${cs} ${verb} *${e.locName}* — ${fmtTime(e.ts)}${from}${note}`;
}
// ---- legs (completed flights) and recaps ----
function buildLegs(events){ // pair each departure with the arrival that follows it, per tail
  const byHex={};for(const e of events){(byHex[e.hex]=byHex[e.hex]||[]).push(e);}
  const legs=[];
  for(const hex in byHex){const list=byHex[hex].sort((a,b)=>a.ts-b.ts);
    for(let i=1;i<list.length;i++){const d=list[i-1],a=list[i];if(d.type==='DEPARTED'&&a.type==='ARRIVED')legs.push(legRecord(d,a));}}
  return legs.sort((a,b)=>b.arr-a.arr);
}
function legRecord(d,a){const A=LOCATIONS.find(l=>l.id===d.locId),B=LOCATIONS.find(l=>l.id===a.locId);
  return{hex:a.hex,reg:a.reg,from:d.locId,fromName:d.locName,to:a.locId,toName:a.locName,dep:d.ts,arr:a.ts,ete:a.ts-d.ts,nm:(A&&B)?+haversineNm(A.lat,A.lon,B.lat,B.lon).toFixed(1):null,est:!!(d.inferred||a.inferred)};}
async function getLegs(env){
  let legs=await env.KV.get('legs','json');
  if(!Array.isArray(legs)){legs=buildLegs((await env.KV.get('events','json'))||[]);await env.KV.put('legs',JSON.stringify(legs));} // one-time backfill from the event log
  return legs;
}
function tzOffsetMs(ts){const m=/(\d+)\/(\d+)\/(\d+), (\d+):(\d+):(\d+)/.exec(new Date(ts).toLocaleString('en-US',{timeZone:TZ,hour12:false,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'}));
  return Date.UTC(+m[3],+m[1]-1,+m[2],+m[4]%24,+m[5],+m[6])-ts;}
function localMidnight(ts){const off=tzOffsetMs(ts),l=new Date(ts+off);return Date.UTC(l.getUTCFullYear(),l.getUTCMonth(),l.getUTCDate())-off;}
function fmtDay(ts){return new Date(ts).toLocaleDateString('en-US',{weekday:'short',month:'numeric',day:'numeric',timeZone:TZ});}
function periodRange(period,now){
  const mid=localMidnight(now);
  if(period==='yesterday')return{start:mid-86400e3,end:mid,label:'yesterday ('+fmtDay(mid-86400e3)+')'};
  if(period==='24h')return{start:now-86400e3,end:now,label:'past 24 hours'};
  return{start:mid,end:now,label:'today ('+fmtDay(mid)+', since midnight)'};
}
// A leg whose time can't be a direct flight (slower than ~50 kt plus 12 min of ground/approach slack) had an untracked stop
// in between. It still counts as a flight, but it is kept out of route ETE averages.
function isIndirect(l){return l.nm!=null&&l.nm>=1&&l.ete>(l.nm/50)*3600e3+12*60e3;}
function recapFromLegs(legs,{hex,name,period,now}){
  const {start,end,label}=periodRange(period,now);
  const sel=legs.filter(l=>l.arr>=start&&l.arr<end&&(!hex||l.hex===hex));
  if(!sel.length)return `*${name} recap — ${label}*\nNo completed flights in this window.`;
  const total=sel.length,air=sel.reduce((a,l)=>a+l.ete,0);
  const routes={};for(const l of sel){const k=l.from+'>'+l.to;const r=routes[k]||(routes[k]={from:l.fromName,to:l.toName,n:0,d:0,ete:0});r.n++;if(!isIndirect(l)){r.d++;r.ete+=l.ete;}}
  const avg=r=>r.d?fmtDur(r.ete/r.d):'n/a';
  const ranked=Object.values(routes).sort((a,b)=>b.n-a.n||(a.d?a.ete/a.d:1e12)-(b.d?b.ete/b.d:1e12)),top=ranked[0];
  const indirect=sel.filter(isIndirect).length;
  const lines=[`*${name} recap — ${label}*`,`${total} flight${total===1?'':'s'} · ${fmtDur(air)} airborne`+(hex?'':` · ${new Set(sel.map(l=>l.hex)).size} tails flew`)];
  lines.push(`Most flown route: *${top.from} → ${top.to}* — ${top.n} flight${top.n===1?'':'s'}, avg ETE ${avg(top)}`);
  if(ranked.length>1)lines.push('Also: '+ranked.slice(1,4).map(r=>`${r.from} → ${r.to} ×${r.n} (avg ${avg(r)})`).join(' · '));
  if(indirect)lines.push(`_${indirect} flight${indirect===1?'':'s'} had an untracked stop en route and ${indirect===1?'is':'are'} left out of the ETE averages_`);
  if(!hex){const per={};for(const l of sel)per[l.reg]=(per[l.reg]||0)+1;lines.push('By tail: '+Object.entries(per).sort((a,b)=>b[1]-a[1]).map(([r,n])=>`${r} ×${n}`).join(' · '));}
  const est=sel.filter(l=>l.est).length;if(est)lines.push(`_${est} of ${total} flights include an estimated time_`);
  return lines.join('\n');
}
async function recapText(env,p,tails){
  let hex=null,name='Fleet';
  if(p.tail){hex=await resolveHex(p.tail);const t=hex&&tails.find(x=>x.hex===hex);if(!t)return `*${p.tail}* isn't on the tracker.`;name=tailName(t);}
  return recapFromLegs(await getLegs(env),{hex,name,period:p.period||'today',now:Date.now()});
}
function fmtDur(ms){const m=Math.max(1,Math.round(ms/60000));return m<60?m+' min':Math.floor(m/60)+' h '+String(m%60).padStart(2,'0')+' min';}
// The leg an arrival completes: the tail's immediately preceding event must be a departure.
function legFor(arr,allEvents){
  const prev=allEvents.filter(e=>e.hex===arr.hex&&e.ts<arr.ts).sort((a,b)=>b.ts-a.ts)[0];
  if(!prev||prev.type!=='DEPARTED')return null;
  const a=LOCATIONS.find(l=>l.id===prev.locId),b=LOCATIONS.find(l=>l.id===arr.locId);
  return{from:prev,to:arr,ete:arr.ts-prev.ts,nm:(a&&b)?haversineNm(a.lat,a.lon,b.lat,b.lon):null,estimated:!!(prev.inferred||arr.inferred)};
}
function legLine(leg){
  return `:arrow_right: Route: *${leg.from.locName}* \u2192 *${leg.to.locName}* \u00b7 ETE ${fmtDur(leg.ete)}`+(leg.nm!=null?` \u00b7 ${leg.nm.toFixed(0)} nm`:'')+(leg.estimated?' \u00b7 _times estimated_':'');
}
async function slackSend(env,text,thread_ts){return slackSendTo(env,env.SLACK_CHANNEL_ID,text,thread_ts);}
async function slackSendTo(env,channel,text,thread_ts){
  if(env.SLACK_BOT_TOKEN&&channel){
    try{const r=await fetch('https://slack.com/api/chat.postMessage',{method:'POST',headers:{'Content-Type':'application/json; charset=utf-8','Authorization':'Bearer '+env.SLACK_BOT_TOKEN},
        body:JSON.stringify({channel,text,thread_ts,unfurl_links:false})});
      const j=await r.json();if(j.ok)return j.ts||true;}catch(e){}
  }
  if(!thread_ts&&env.SLACK_WEBHOOK&&channel===env.SLACK_CHANNEL_ID){ // fallback: webhook (cannot thread)
    try{const r=await fetch(env.SLACK_WEBHOOK,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text,unfurl_links:false})});return r.ok?true:null;}catch(e){}
  }
  return null;
}
// KV is eventually consistent, so a run can occasionally miss an event the previous run just recorded and re-detect it.
// Slack itself is consistent: skip any line whose tail/action/place already appeared in the channel in the last 5 minutes.
async function recentSlackKeys(env){
  if(!env.SLACK_BOT_TOKEN||!env.SLACK_CHANNEL_ID)return[];
  try{const r=await fetch('https://slack.com/api/conversations.history?'+new URLSearchParams({channel:env.SLACK_CHANNEL_ID,oldest:String((Date.now()-5*60e3)/1000),limit:'50'}),{headers:{'Authorization':'Bearer '+env.SLACK_BOT_TOKEN}});
    const j=await r.json();return j.ok?(j.messages||[]).map(m=>(m.text||'').split(' — ')[0]):[];}catch(e){return[];}
}
async function postSlack(env,newEvents,allEvents){
  let ok=true;
  const seen=newEvents.length?await recentSlackKeys(env):[];
  for(const e of newEvents.slice().sort((a,b)=>a.ts-b.ts)){
    const leg=e.type==='ARRIVED'?legFor(e,allEvents):null;
    const line=slackLine(e,leg),key=line.split(' — ')[0];
    if(seen.includes(key))continue; // already in the channel: a repeat, not a new event
    seen.push(key);
    const ts=await slackSend(env,line);
    if(ts===null)ok=false;else if(typeof ts==='string')e.slackTs=ts;
  }
  return ok;
}
async function slackEdit(env,ts,text){
  if(!env.SLACK_BOT_TOKEN||!env.SLACK_CHANNEL_ID||!ts)return false;
  try{const r=await fetch('https://slack.com/api/chat.update',{method:'POST',headers:{'Content-Type':'application/json; charset=utf-8','Authorization':'Bearer '+env.SLACK_BOT_TOKEN},body:JSON.stringify({channel:env.SLACK_CHANNEL_ID,ts,text})});return (await r.json()).ok;}catch(e){return false;}
}
function struck(e,why){return `~*${e.reg}* ${e.type==='ARRIVED'?'arrived at':'departed'} *${e.locName}* — ${fmtTime(e.ts)}~ · _corrected: ${why}_`;}
// Remove withdrawn events from the log and the leg history, and strike their Slack posts.
async function applyRetractions(env,events,list){
  if(!list||!list.length)return;
  const legs=await getLegs(env);let legsChanged=false;
  for(const r of list){
    const i=events.findIndex(e=>e.hex===r.hex&&e.type==='ARRIVED'&&e.ts===r.ts&&(!r.locId||e.locId===r.locId));if(i<0)continue;
    const [e]=events.splice(i,1);
    const j=legs.findIndex(l=>l.hex===r.hex&&l.arr===r.ts&&(!r.locId||l.to===r.locId));if(j>=0){legs.splice(j,1);legsChanged=true;}
    if(e.slackTs)await slackEdit(env,e.slackTs,struck(e,r.why||'it flew past without landing'));
  }
  if(legsChanged)await env.KV.put('legs',JSON.stringify(legs));
}

export {passedOver,inferStop,landingCandidate,touchdownLoc,flewThrough,flightStatus,stepTail,qnhFor,indAlt,aglAt,confirmDep,wxChangeLine,wxSummary,wxName,latestByStation,wxStatusText,verifySlack,nToHex,hexToN,resolveHex,parseCommand,describePos,describeTail,pushEvent,detectEvents,handleLost,legFor,legLine,getStatus,buildLegs,recapFromLegs,periodRange,isIndirect};

// Ground Connect (blade-ground-connect): its own cron trigger never fired, so this worker's proven cron kicks its sweep twice a minute via a service binding.
async function kickGroundConnect(env){
  if(!env.GC||!env.GC_KEY)return;
  for(let i=0;i<2;i++){
    if(i)await new Promise(r=>setTimeout(r,30000));
    try{await env.GC.fetch('https://ground-connect/sweep?key='+encodeURIComponent(env.GC_KEY));}catch(e){console.error('gc kick',e);}
  }
}
