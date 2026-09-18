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
const LOST_SEC=90,ARRIVE_MAX_ALT=1500,DEPART_MAX_ALT=2500,EVENT_GAP_SEC=300,LK_MAX_H=48;
const FEEDS=['https://api.adsb.lol/v2/icao/','https://opendata.adsb.fi/api/v2/icao/'];
const TRACE_BASE='https://adsb.lol/data/traces/',TRACE_EVERY_MS=5*60e3;
// The feeds rate-limit / block Cloudflare's shared egress IPs, so upstream requests go through the Vercel proxy
// (same one the dashboard uses). It only forwards to the allow-listed feed hosts.
const PROXY='https://blade-fleet-ops.vercel.app/api/proxy?u=';
function upstream(u,ms){return fetch(PROXY+encodeURIComponent(u),{headers:{'Accept':'application/json'},signal:AbortSignal.timeout(ms||10000)});}
const MAX_EVENTS=150,SAMPLE_GAP_MS=30000; // keep the stored log modest: the free plan allows 10 ms CPU per run
const TZ='America/New_York';

export default {
  async scheduled(event,env,ctx){ctx.waitUntil(runSweeps(env));},
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
        body:JSON.stringify({channel:ch.startsWith('#')?ch:'#'+ch,text:':white_check_mark: blade_fleet_tracker can post here. Mention me with `list`, `add N12345 [label]`, `remove N12345`, or `status`.'})});
      const j=await r.json();return new Response(JSON.stringify({ok:j.ok,error:j.error,channel:j.channel}),{headers:cors});
    }
    if(url.pathname==='/locations')return new Response(JSON.stringify(await getLocations(env)),{headers:cors});
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
  await sweep(env);
  await new Promise(r=>setTimeout(r,SAMPLE_GAP_MS));
  await sweep(env);
}

// ---- geometry / status (same as index.html) ----
function haversineNm(la1,lo1,la2,lo2){const R=3440.065,r=Math.PI/180;const a=Math.sin((la2-la1)*r/2)**2+Math.cos(la1*r)*Math.cos(la2*r)*Math.sin((lo2-lo1)*r/2)**2;return R*2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));}
function nearestLoc(lat,lon){if(lat==null||lon==null)return null;let best=null,bd=Infinity;for(const L of LOCATIONS){const d=haversineNm(lat,lon,L.lat,L.lon);if(d<bd){bd=d;best=L;}}if(bd>MAX_NEAR_NM)return null;return{loc:best,dist:bd};}
function snapNear(n){return n?{id:n.loc.id,name:n.loc.name,r:n.loc.r,dist:n.dist}:null;}
function nearForEvent(s){return !!s&&s.dist<=s.r+(s.r<1?0.5:1.0);}
function getStatus(ac){if(!ac.lat||ac.seen>120)return'stale';if(ac.alt_baro==='ground')return'ground';if(typeof ac.alt_baro==='number'&&ac.alt_baro>0)return'airborne';if(ac.gs>30)return'airborne';return'ground';}
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
function detectEvents(t,ac,prev,status,alt,near,now,events,out){
  if(!prev)return;
  const wasDown=prev.status==='ground'||prev.status==='nosignal';
  if(wasDown&&status==='airborne'){
    let from=null;
    if(nearForEvent(prev.nearest)&&lkFresh(prev,now))from=prev.nearest;
    else if(alt!=null&&alt<=DEPART_MAX_ALT&&nearForEvent(near))from=near;
    if(from)pushEvent(events,out,'DEPARTED',t,ac,from,now,prev.status==='nosignal');
  }else if(prev.status==='airborne'&&status==='ground'){
    if(nearForEvent(near))pushEvent(events,out,'ARRIVED',t,ac,near,now,false);
  }else if(prev.status==='airborne'&&status==='airborne'&&alt!=null&&prev.alt!=null){
    if(prev.alt<=EVENT_ALT&&alt>EVENT_ALT&&nearForEvent(prev.nearest))pushEvent(events,out,'DEPARTED',t,ac,prev.nearest,now,false);
    else if(prev.alt>EVENT_ALT&&alt<=EVENT_ALT&&nearForEvent(near))pushEvent(events,out,'ARRIVED',t,ac,near,now,false);
  }
}
function handleLost(t,prev,now,events,out){
  if(!prev||prev.status==='nosignal')return;
  if(now-prev.lastSeen<LOST_SEC*1000)return;
  if(prev.status==='airborne'&&prev.alt!=null&&prev.alt<=ARRIVE_MAX_ALT&&nearForEvent(prev.nearest))
    pushEvent(events,out,'ARRIVED',t,prev,prev.nearest,now,true);
  prev.status='nosignal';prev.ts=now;
}
function pushEvent(events,out,type,t,ac,loc,now,inferred){
  if([...out,...events].find(e=>e.hex===t.hex&&e.type===type&&now-(e.ts||0)<EVENT_GAP_SEC*1000))return;
  out.push({type,hex:t.hex,reg:regOf(t.hex,ac,t.label),label:t.label||'',locName:loc.name,locId:loc.id,dist:+loc.dist.toFixed(2),
    callsign:(ac.flight||'').trim(),acType:ac.t||'',alt:typeof ac.alt_baro==='number'?Math.round(ac.alt_baro):(ac.alt_baro==='ground'?0:null),
    ts:now,inferred:!!inferred});
}

// ---- locations (locations.json on GitHub Pages, cached in KV, refreshed every 5 min) ----
async function getLocations(env){
  const now=Date.now(),cached=await env.KV.get('locations','json');
  if(cached&&cached.list&&cached.list.length&&now-(cached.fetchedAt||0)<LOCATIONS_EVERY_MS){LOCATIONS=cached.list;return LOCATIONS;}
  try{
    const r=await fetch(LOCATIONS_URL+'?ts='+now,{signal:AbortSignal.timeout(8000)});
    if(r.ok){const list=await r.json();
      const clean=(Array.isArray(list)?list:[]).filter(l=>l&&l.id&&typeof l.lat==='number'&&typeof l.lon==='number').map(l=>({id:l.id,name:l.name||l.id,lat:l.lat,lon:l.lon,r:l.r||2.0}));
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
const KEYWORDS=[[/\b(add|track|start tracking|watch|include)\b/i,'add'],[/\b(remove|delete|untrack|stop tracking|drop|exclude)\b/i,'remove'],
  [/\b(where|find|locate|position of)\b/i,'lookup'],[/\b(list|fleet|tails|tracking)\b/i,'list'],[/\b(status|health|alive|working)\b/i,'status'],[/\b(help|commands)\b/i,'help']];
function findTail(text){
  const m=/\b(N[1-9][0-9]{0,4}[A-Z]{0,2})\b/i.exec(text)||/\b([0-9A-F]{6})\b/i.exec(text);
  return m?{token:m[1].toUpperCase(),index:m.index,length:m[1].length}:null;
}
function countTails(text){return new Set([...text.matchAll(/\b(N[1-9][0-9]{0,4}[A-Z]{0,2})\b/gi)].map(m=>m[1].toUpperCase())).size;}
const LABEL_MAX=40;
function parseCommand(rawText,mentioned){
  const text=(rawText||'').replace(/<@[^>]+>/g,' ').replace(/[‘’]/g,"'").replace(/\s+/g,' ').trim();
  const tail=findTail(text);
  const words=text.split(' ').filter(Boolean);
  // Guard rails so instructions, examples and chatter are never treated as commands:
  //  - quoted command examples ("add N123AB ...") or bullet lists are documentation, not requests
  //  - a message naming several different tails is ambiguous
  //  - a plain (un-mentioned) request must be short and start with the command word
  if(/["'`\u201c]\s*(add|remove|track|untrack|delete|where|list|status)\b/i.test(text)||/[\u2022]/.test(text))return null;
  if(countTails(text)>1&&!mentioned)return null;
  if(!mentioned&&(words.length>12||!KEYWORDS.some(([re])=>re.test(words.slice(0,3).join(' ')))))return null;
  let cmd=null;for(const [re,name] of KEYWORDS){if(re.test(text)){cmd=name;break;}}
  if(cmd==='list'&&tail)cmd=null; // "who is N84BL" -> a lookup, not the fleet list
  if(!cmd&&tail)cmd='lookup';
  if(!cmd)return mentioned?{cmd:'help',text}:null;
  // Without a direct mention, a bare list/status/help only counts when the message is short and clearly aimed at the bot.
  if(!mentioned&&!tail&&(cmd==='list'||cmd==='status'||cmd==='help')&&text.split(' ').length>4)return null;
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
  const st=!s?'no data yet':s.status==='airborne'?'airborne'+(s.alt!=null?' at '+Math.round(s.alt).toLocaleString()+' ft':''):s.status==='ground'?'on the ground':'not transmitting · last tracked '+fmtTime(s.lastSeen);
  let where='';
  if(s&&s.lat!=null){
    const n=s.nearest;
    if(n&&n.dist<=n.r)where=` · at ${n.name}`;
    else{const d=describePos(s.lat,s.lon,s.track,s.status==='airborne');
      where=` · ${d.where}`+(d.hdg?`, heading ${d.hdg}`:'')+(d.inArea&&d.station?` · ${d.station.dist.toFixed(0)} nm from ${d.station.loc.id}`:'');}
  }
  return `*${tailName(t)}*${t.label?' ('+t.label+')':''} — ${st}${where}`;
}
async function runCommand(env,p){
  const tails=await getTails(env);await getLocations(env);await getWaypoints(env);
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
  if(p.cmd==='status'){
    const h=(await env.KV.get('health','json'))||{};
    return `Last sweep ${h.lastRun?fmtTime(h.lastRun):'never'} · ${h.ok?'feeds ok':'feeds unavailable'} · ${h.live??0} tails live · ${h.events??0} events logged · Slack posting ${h.slackOk===false?'failed':'ok'}`;
  }
  return 'I track the BLADE fleet and post departures and arrivals here. Just tell me in plain words:\n• "add N84BL Robby\'s 407" — start tracking a tail (label optional)\n• "remove N84BL" — stop tracking\n• "where is N84BL" — one tail\'s status\n• "list" — every tail\n• "status" — tracker health';
}
// Channel polling: independent of Slack's event delivery. Reads new messages in the tracker channel each sweep.
async function pollSlackChannel(env){
  if(!env.SLACK_BOT_TOKEN||!env.SLACK_CHANNEL_ID)return;
  const cursor=await env.KV.get('slack_cursor');
  const params=new URLSearchParams({channel:env.SLACK_CHANNEL_ID,limit:'20'});
  if(cursor)params.set('oldest',cursor);
  const r=await fetch('https://slack.com/api/conversations.history?'+params,{headers:{'Authorization':'Bearer '+env.SLACK_BOT_TOKEN}});
  const j=await r.json();
  if(!j.ok){await env.KV.put('slack_poll_error',j.error||'unknown');return;}
  await env.KV.delete('slack_poll_error');
  const msgs=(j.messages||[]).filter(m=>m.type==='message'&&!m.bot_id&&!m.subtype&&m.text&&m.ts!==cursor).sort((a,b)=>+a.ts-+b.ts);
  if(!cursor){ // first run: only mark the position, never replay old history
    const latest=(j.messages||[]).reduce((mx,m)=>+m.ts>+mx?m.ts:mx,'0');
    await env.KV.put('slack_cursor',latest===  '0'?String(Date.now()/1000):latest);return;
  }
  let last=cursor;
  for(const m of msgs){await handleMessage(env,{...m,channel:env.SLACK_CHANNEL_ID},false);if(+m.ts>+last)last=m.ts;}
  const newest=(j.messages||[]).reduce((mx,m)=>+m.ts>+mx?m.ts:mx,last);
  if(newest!==cursor)await env.KV.put('slack_cursor',newest);
}
async function slackPost(env,channel,text,thread_ts){
  if(!env.SLACK_BOT_TOKEN)return;
  await fetch('https://slack.com/api/chat.postMessage',{method:'POST',headers:{'Content-Type':'application/json; charset=utf-8','Authorization':'Bearer '+env.SLACK_BOT_TOKEN},
    body:JSON.stringify({channel,text,thread_ts,unfurl_links:false})});
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
  const silent=tails.filter(t=>{const p=state[t.hex];return !p||(p.status!=='airborne'&&p.status!=='ground');});
  await Promise.all(silent.map(async t=>{
    try{
      const r=await upstream(TRACE_BASE+t.hex.slice(-2)+'/trace_recent_'+t.hex+'.json',12000);
      if(!r.ok)return;const d=await r.json();const tr=d.trace||[];if(!tr.length)return;
      const last=tr[tr.length-1],ts=Math.round((d.timestamp+last[0])*1000),p=state[t.hex];
      if(p&&p.lastSeen&&p.lastSeen>=ts)return;
      const lat=last[1],lon=last[2],alt=last[3]==='ground'?0:(typeof last[3]==='number'?last[3]:null);
      state[t.hex]={status:'nosignal',alt,alt_baro:last[3],lat,lon,nearest:snapNear(nearestLoc(lat,lon)),lastSeen:ts,ts:now,
        r:d.r||(p&&p.r)||'',t:d.t||(p&&p.t)||'',desc:d.desc||(p&&p.desc)||'',flight:'',fromTrace:true};
    }catch(e){}
  }));
}

async function sweep(env){
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
  for(const t of tails){
    const ac=byHex[t.hex],prev=state[t.hex];
    const status=ac?getStatus(ac):'stale';
    if(ac&&status!=='stale'){
      const alt=altOf(ac),near=snapNear(nearestLoc(ac.lat,ac.lon));
      detectEvents(t,ac,prev,status,alt,near,now,events,out);
      state[t.hex]={status,alt,alt_baro:ac.alt_baro,lat:ac.lat,lon:ac.lon,nearest:near,
        lastSeen:now-Math.round((ac.seen_pos!=null?ac.seen_pos:(ac.seen||0))*1000),ts:now,track:typeof ac.track==='number'?ac.track:null,
        r:ac.r||(prev&&prev.r)||'',t:ac.t||(prev&&prev.t)||'',desc:ac.desc||(prev&&prev.desc)||'',flight:(ac.flight||'').trim()};
    }else handleLost(t,prev,now,events,out);
  }
  let lastTrace=health.lastTrace||0;
  if(now-lastTrace>TRACE_EVERY_MS){await backfillTraces(state,now,tails);lastTrace=now;}
  let slackOk=health.slackOk;
  if(out.length){
    events.unshift(...out.slice().reverse());
    events.length=Math.min(events.length,MAX_EVENTS);
    slackOk=await postSlack(env,out);
  }
  await env.KV.put('state',JSON.stringify(state));
  await env.KV.put('events',JSON.stringify(events));
  await env.KV.put('health',JSON.stringify({lastRun:now,ok:true,live:fresh.length,lastTrace,slackOk,events:events.length,slackPollError:(await env.KV.get('slack_poll_error'))||null}));
  try{await pollSlackChannel(env);}catch(e){await env.KV.put('slack_poll_error',String(e.message||e));}
}

// ---- Slack ----
function fmtTime(ts){return new Date(ts).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZone:TZ});}
function slackLine(e){
  const arrow=e.type==='DEPARTED'?':small_red_triangle:':':large_green_circle:'; // red up-triangle = departed, green circle = arrived
  const verb=e.type==='DEPARTED'?'departed':'arrived at';
  const alt=e.alt==null?'':e.alt===0?' · on ground':' · '+e.alt.toLocaleString()+' ft';
  const note=e.inferred?' · _estimated from last tracked position_':'';
  const cs=e.callsign&&e.callsign!==e.reg?' ('+e.callsign+')':'';
  return `${arrow} *${e.reg}*${cs} ${verb} *${e.locName}* — ${fmtTime(e.ts)}${alt} · ${e.dist} nm${note}`;
}
async function postSlack(env,newEvents){
  if(!env.SLACK_WEBHOOK)return false;
  try{
    const r=await fetch(env.SLACK_WEBHOOK,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({text:newEvents.map(slackLine).join('\n'),unfurl_links:false})});
    return r.ok;
  }catch(e){return false;}
}

export {verifySlack,nToHex,hexToN,resolveHex,parseCommand,describePos,describeTail};
