#!/usr/bin/env python
import sys
import json
import os
import requests
from ripe.atlas.cousteau import ProbeRequest
import urllib2

## where the member lists live
SOURCE_URL=u'http://ml.ix-f.net/api/directory'

### reach defines close-by-probes
REACH_KM=50
# after chat with Max 50 seems reasonable. thinking of topix vs mix

### globals
prb_id2info = {}
coord2prb_ids = {}
###

def locstr2latlng( locstring ):
   locstr = urllib2.quote( locstring )
   geocode_url = u'http://maps.googleapis.com/maps/api/geocode/json?address=%s&sensor=false' % locstr
   req = requests.get( geocode_url )
   resp = req.json()
   ll = resp['results'][0]['geometry']['location']
   return ( ll['lat'], ll['lng'] )

def atlas_probelist_countries( cc_list ): 
   probes = []
   for cc in cc_list:
      ps = ProbeRequest(country_code=cc,status=1)
      for p in ps:
         probes.append( p )
   return probes

def process_ixp_org( j ):
   '''
   takes ixp json and produces interesting stuff from it
   '''
   d = {
      'ix2as': {},
      'ix2prb': {}
   }
   for m in j['member_list']:
      asn = m['asnum']
      for conn in m['connection_list']:
         ixp_id = conn['ixp_id']
         d['ix2as'].setdefault( ixp_id, set() )
         d['ix2as'][ixp_id].add( asn )
   for k,v in d['ix2as'].iteritems():
         print "ixp_id: %d , number_of_asns: %d" % ( k, len(v) )
   for ixp in j['ixp_list']:
      try:
         ixp_id = ixp['ixp_id'] # this is the internal to the ML-JSON match
         ixf_id = ixp['ixf_id'] # this is the IX-F external ID of the IXP
         locs = set()
         coords = []
         probe_ids = set()
         for s in ixp['switch']:
            city = s['city']
            country = s['country']
            locs.add( "%s,%s" % (city,country) )
         locs = list( locs )
         print "locs: %s" % ( locs )
         ## now find probes near these locs 
         for l in locs:
            coord = locstr2latlng( l )
            coords.append( coord )
         for coord in coords:
            rad_args = '%s,%s:%s' % ( coord[0], coord[1], REACH_KM ) 
            #print "COORDS: %s" % ( rad_args )
            prb_list = ProbeRequest(radius=rad_args)
            for p in prb_list:
               prb_id2info[ p['id'] ] = p
               probe_ids.add( p['id'] )
         #
         print "found %d probes for ixp %d" % ( len(probe_ids), ixp_id )
      except:
         pass


   
if __name__ == "__main__":
   req_src = requests.get( SOURCE_URL )
   for ixp_entry in req_src.json():
      ixp_name = ixp_entry['name'] 
      print >>sys.stderr, "processing ixp: %s" % ixp_name
      for ixp_source in ixp_entry['sources']:
         if ixp_source['reachable'] and ixp_source['valid']:
            ixp_src =  requests.get( ixp_source['url'] )
            process_ixp_org( ixp_src.json() )

# OLD stuff
'''
   for ix_entry in ixp.data['ixp_list']:
      ixp_id = ix_entry['ixp_id']
      asn_member_info = {}
      countries=set()
      if 'country' in ix_entry:
         countries.add( ix_entry['country'] )
      if 'switch' in ix_entry:
         for switch in ix_entry['switch']:
            if 'country' in switch:
               countries.add( switch['country'] )
      prb_info = atlas_probelist_countries( list( countries ) )
      for member in ixp.data['member_list']: 
         for conn in member['connection_list']:
            if conn['ixp_id'] == ixp_id:
               asn_member_info[ member['asnum'] ] = member
               #print "%s %s" % ( conn, member['asnum'] )
      # collected all info in prb_info and asn_member_info 
      asns = {4:{},6:{}}
      for prb in prb_info: 
         if prb['asn_v4'] != None:
            asns[4].setdefault( prb['asn_v4'], {'probes': [], 'vlans': set() } )
            asns[4][ prb['asn_v4'] ]['probes'].append( prb )
         if prb['asn_v6'] != None:
            asns[6].setdefault( prb['asn_v6'], {'probes': [], 'vlans': set() } )
            asns[6][ prb['asn_v6'] ]['probes'].append( prb )
      for member in asn_member_info.values():
         #asns[4].setdefault( prb['asn_v4'], {'probes': [], 'ixp': {} } )
         for member in ixp.data['member_list']: 
            member_asn = member['asnum']
            for conn in member['connection_list']:
               for vlan in conn['vlan_list']:
                  if 'ipv4' in vlan:
                     asns[4].setdefault( member_asn, {'probes': [], 'vlans': set()  } )
                     asns[4][ member_asn ]['vlans'].add( vlan['vlan_id'] )
                  if 'ipv6' in vlan:
                     asns[6].setdefault( member_asn, {'probes': [], 'vlans': set() } )
                     asns[6][ member_asn ]['vlans'].add( vlan['vlan_id'] )
      ### now display asns info
      asn_list = set()
      for af in (4,6):
         for asn in asns[af].keys():
            asn_list.add( asn )
      asn_type = {
         'ix+prb+': [],
         'ix+prb-': [],
         'ix-prb+': [],
         'ix-prb-': [] # can't happen!
      }
      asn_type2label = {
         'ix+prb+': "ASNs at IXP with probes in same country",
         'ix+prb-': "ASNs at IXP without probes in same country",
         'ix-prb+': "ASNs not at IXP with probes in same country"
      }
      for asn in asn_list:
         stats = {
            'asn': asn,
            'v4_prbs': 0,
            'v6_prbs': 0,
            'v4_ixp': False,
            'v6_ixp': False
         }
         has_prbs=False
         on_ixp=False
         try:
            stats['v4_prbs'] = len( asns[4][ asn ]['probes'] )
         except: pass
         try:
            stats['v6_prbs'] = len( asns[6][ asn ]['probes'] )
         except: pass
         if stats['v4_prbs'] + stats['v6_prbs'] > 0:
            has_prbs=True
         try:
            if len( asns[4][ asn ]['vlans'] ) > 0:
               stats['v4_ixp'] = True
               on_ixp = True
         except: pass
         try:
            if len( asns[6][ asn ]['vlans'] ) > 0:
               stats['v6_ixp'] = True
               on_ixp = True
         except: pass
         if   has_prbs == True  and on_ixp == True:
            asn_type['ix+prb+'].append( stats )
         elif has_prbs == False and on_ixp == True:
            asn_type['ix+prb-'].append( stats )
         elif has_prbs == True  and on_ixp == False:
            asn_type['ix-prb+'].append( stats )
         else:
            # can't happen
            print "THIS CANT HAPPEN!"
            raise
      for typ in ('ix+prb+','ix-prb+','ix+prb-'):
         print "%s" % ( asn_type2label[typ], )
         print "%s" % ( '=' * len(asn_type2label[typ]), )
         print "ASN count: %s" % ( len( asn_type[typ] ) )
         entries = sorted( asn_type[typ], key=lambda x: x['asn'] )
         for x in entries:
            prb_str = " "
            if x['v4_prbs'] + x['v6_prbs'] > 0:
               prb_str = "probes: %s(v4) %s(v6) " % ( x['v4_prbs'], x['v6_prbs'] )
            print "AS{} {} ixp4:{} ixp6:{}".format(
               x['asn'],
               prb_str,
               x['v4_ixp'],
               x['v6_ixp']
         )
         print "\n\n"
'''
