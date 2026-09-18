#!/usr/bin/env python3
"""Add airports/heliports to locations.json by FAA/ICAO code, using OurAirports open data.

Usage:  python3 tools/resolve_locations.py 65NJ KHPN 6N5 [--dry-run]
        python3 tools/resolve_locations.py --file codes.txt      (one code per line, optional "CODE Custom Name")

Radius defaults: heliport 0.5 nm, small airport 1.5 nm, medium/large airport 2.0 nm. Existing entries are kept.
"""
import csv,io,json,os,sys,urllib.request,time
CSV_URL='https://davidmegginson.github.io/ourairports-data/airports.csv'
CACHE='/tmp/ourairports_airports.csv'
HERE=os.path.dirname(os.path.abspath(__file__)); LOC=os.path.join(HERE,'..','locations.json')
def load_csv():
    if not os.path.exists(CACHE) or time.time()-os.path.getmtime(CACHE)>7*86400:
        urllib.request.urlretrieve(CSV_URL,CACHE)
    return list(csv.DictReader(open(CACHE,encoding='utf-8')))
def radius(t): return 0.5 if t=='heliport' else 2.0 if t in('large_airport','medium_airport') else 1.5
def find(rows,code):
    c=code.upper()
    for key in ('local_code','gps_code','icao_code','iata_code','ident'):
        for r in rows:
            if (r.get(key) or '').upper()==c and r.get('iso_country')=='US': return r
    for r in rows:
        if c in {(r.get(k) or '').upper() for k in ('local_code','gps_code','icao_code','iata_code','ident')}: return r
    return None
def main():
    args=[a for a in sys.argv[1:] if not a.startswith('--')]; dry='--dry-run' in sys.argv
    if '--file' in sys.argv:
        f=sys.argv[sys.argv.index('--file')+1]; args=[l.strip() for l in open(f) if l.strip() and not l.startswith('#')]
    if not args: print(__doc__); return
    rows=load_csv(); locs=json.load(open(LOC,encoding='utf-8')); have={l['id'].upper() for l in locs}
    added=[]
    for entry in args:
        parts=entry.split(None,1); code=parts[0].upper(); custom=parts[1].strip() if len(parts)>1 else None
        r=find(rows,code)
        if not r: print(f'  !! {code}: not found in OurAirports'); continue
        ident=(r.get('local_code') or r.get('gps_code') or r.get('ident')).upper()
        if ident in have or code in have: print(f'  == {code}: already in locations.json'); continue
        lat,lon=float(r['latitude_deg']),float(r['longitude_deg'])
        near=[l for l in locs if abs(l['lat']-lat)*60<0.3 and abs(l['lon']-lon)*60*0.76<0.3]  # same spot under another id
        if near: print(f"  == {code}: already present as {near[0]['id']} ({near[0]['name']})"); continue
        name=custom or f"{r['name']} ({ident})"
        loc={'id':ident,'name':name,'lat':round(float(r['latitude_deg']),4),'lon':round(float(r['longitude_deg']),4),'r':radius(r['type'])}
        locs.append(loc); have.add(ident); added.append(loc)
        print(f"  ++ {loc['id']:6s} {loc['name']:40s} {loc['lat']:.4f}, {loc['lon']:.4f}  r={loc['r']}  ({r['type']}, {r.get('municipality','')})")
    if added and not dry:
        json.dump(locs,open(LOC,'w',encoding='utf-8'),indent=1,ensure_ascii=False); open(LOC,'a').write('\n')
        print(f'locations.json now has {len(locs)} entries')
    elif dry: print('(dry run: nothing written)')
main()
