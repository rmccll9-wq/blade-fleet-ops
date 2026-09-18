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
const LOCATIONS=[
  {id:'JRB',name:'Downtown Manhattan HP',lat:40.7011,lon:-74.0090,r:0.5},
  {id:'E34',name:'East 34th St Heliport',lat:40.7428,lon:-73.9722,r:0.5},
  {id:'W30',name:'West 30th St Heliport',lat:40.7544,lon:-74.0072,r:0.5},
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
const EVENT_ALT=500,MAX_NEAR_NM=10;
const LOST_SEC=90,ARRIVE_MAX_ALT=1500,DEPART_MAX_ALT=2500,EVENT_GAP_SEC=300,LK_MAX_H=48;
const FEEDS=['https://api.adsb.lol/v2/icao/','https://opendata.adsb.fi/api/v2/icao/'];
const TRACE_BASE='https://adsb.lol/data/traces/',TRACE_EVERY_MS=5*60e3;
// The feeds rate-limit / block Cloudflare's shared egress IPs, so upstream requests go through the Vercel proxy
// (same one the dashboard uses). It only forwards to the allow-listed feed hosts.
const PROXY='https://blade-fleet-ops.vercel.app/api/proxy?u=';
function upstream(u,ms){return fetch(PROXY+encodeURIComponent(u),{headers:{'Accept':'application/json'},signal:AbortSignal.timeout(ms||10000)});}
const MAX_EVENTS=300,SAMPLE_GAP_MS=30000;
const TZ='America/New_York';

export default {
  async scheduled(event,env,ctx){ctx.waitUntil(runSweeps(env));},
  async fetch(req,env,ctx){
    const url=new URL(req.url);
    const cors={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET','Cache-Control':'no-store','Content-Type':'application/json'};
    if(req.method==='OPTIONS')return new Response(null,{headers:cors});
    if(url.pathname==='/slack/events'&&req.method==='POST')return handleSlack(req,env,ctx);
    if(url.pathname==='/tails')return new Response(JSON.stringify(await getTails(env)),{headers:cors});
    if(url.pathname==='/events')return new Response(JSON.stringify((await env.KV.get('events','json'))||[]),{headers:cors});
    if(url.pathname==='/state')return new Response(JSON.stringify((await env.KV.get('state','json'))||{}),{headers:cors});
    if(url.pathname==='/debug'){
      const batch=(await getTails(env)).map(t=>t.hex).join(',').toUpperCase(),res={};
      for(const base of FEEDS){try{const r=await upstream(base+batch,12000);res[base]={status:r.status,body:(await r.text()).slice(0,160)};}catch(e){res[base]={error:String(e)};}}
      return new Response(JSON.stringify(res),{headers:cors});
    }
    if(url.pathname==='/health')return new Response(JSON.stringify((await env.KV.get('health','json'))||{lastRun:null}),{headers:cors});
    return new Response('BLADE Fleet Ops event worker. Endpoints: /events /state /tails /health',{headers:{'Content-Type':'text/plain'}});
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
  if(payload.type==='event_callback'&&payload.event&&payload.event.type==='app_mention'&&!payload.event.bot_id)
    ctx.waitUntil(handleMention(env,payload.event));
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
async function handleMention(env,ev){
  const text=(ev.text||'').replace(/<@[^>]+>/g,' ').replace(/[\u2018\u2019]/g,"'").replace(/\s+/g,' ').trim();
  let reply;
  try{reply=await runCommand(env,text);}catch(e){reply='Something went wrong: '+e.message;}
  await slackPost(env,ev.channel,reply,ev.thread_ts||ev.ts);
}
function tailName(t){return hexToN(t.hex)||t.hex.toUpperCase();}
async function runCommand(env,text){
  const parts=text.split(' ').filter(Boolean),c=(parts[0]||'').toLowerCase(),rest=parts.slice(1);
  const tails=await getTails(env);
  if(c==='add'||c==='track'){
    if(!rest.length)return 'Usage: `add N12345 [label]`';
    const hex=await resolveHex(rest[0]),label=rest.slice(1).join(' ');
    if(!hex)return `Couldn't resolve *${rest[0]}* to an aircraft. Use a US N-number (e.g. N84BL) or a 6-character ICAO hex.`;
    const existing=tails.find(t=>t.hex===hex);
    if(existing){if(label&&label!==existing.label){existing.label=label;await env.KV.put('tails',JSON.stringify(tails));return `*${tailName(existing)}* was already tracked; label updated to "${label}".`;}return `*${tailName(existing)}* is already tracked.`;}
    tails.push({hex,label});await env.KV.put('tails',JSON.stringify(tails));
    return `:white_check_mark: Added *${hexToN(hex)||hex.toUpperCase()}*${label?' ('+label+')':''} · hex ${hex.toUpperCase()}. Now tracking ${tails.length} tails. It appears on the dashboard within a minute.`;
  }
  if(c==='remove'||c==='delete'||c==='untrack'){
    if(!rest.length)return 'Usage: `remove N12345`';
    const hex=await resolveHex(rest[0]),i=hex?tails.findIndex(t=>t.hex===hex):-1;
    if(i<0)return `*${rest[0]}* isn't on the list.`;
    const [t]=tails.splice(i,1);await env.KV.put('tails',JSON.stringify(tails));
    return `:wastebasket: Removed *${tailName(t)}*. Now tracking ${tails.length} tails.`;
  }
  if(c==='list'||c==='tails'||c==='fleet'){
    const state=(await env.KV.get('state','json'))||{};
    return `*Tracking ${tails.length} tails*\n`+tails.map(t=>{const s=state[t.hex];
      const st=!s?'no data yet':s.status==='airborne'?'airborne'+(s.alt!=null?' '+Math.round(s.alt).toLocaleString()+' ft':''):s.status==='ground'?'on ground':'no signal · last seen '+fmtTime(s.lastSeen);
      const loc=s&&s.nearest?` · ${s.nearest.name} ${s.nearest.dist.toFixed(1)} nm`:'';
      return `• *${tailName(t)}*${t.label?' ('+t.label+')':''} — ${st}${loc}`;}).join('\n');
  }
  if(c==='status'||c==='health'){
    const h=(await env.KV.get('health','json'))||{};
    return `Last sweep ${h.lastRun?fmtTime(h.lastRun):'never'} · ${h.ok?'feeds ok':'feeds unavailable'} · ${h.live??0} tails live · ${h.events??0} events logged · Slack ${h.slackOk===false?'post failed':'ok'}`;
  }
  return 'I track the BLADE fleet. Commands:\n• `add N12345 [label]` — start tracking a tail\n• `remove N12345` — stop tracking\n• `list` — every tail and where it is\n• `status` — worker health';
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
        lastSeen:now-Math.round((ac.seen_pos!=null?ac.seen_pos:(ac.seen||0))*1000),ts:now,
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
  await env.KV.put('health',JSON.stringify({lastRun:now,ok:true,live:fresh.length,lastTrace,slackOk,events:events.length}));
}

// ---- Slack ----
function fmtTime(ts){return new Date(ts).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZone:TZ});}
function slackLine(e){
  const arrow=e.type==='DEPARTED'?':small_red_triangle:':':small_red_triangle_down:';
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

export {verifySlack,nToHex,hexToN,resolveHex};
