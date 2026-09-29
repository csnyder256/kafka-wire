import copy,json,unittest
import client_check as checks

class ReceiptTests(unittest.TestCase):
 def receipt(self,language='java'):
  return {'schema':'kafka-wire.client-check','version':1,'language':language,'client':checks.CLIENTS[language],'client_version':'4.1.2','status':'passed','records':5,'checks':sorted(checks.BASE_CHECKS|{'classic-group','offset-commit-fetch'}),'settings':{'idempotence':False,'compression':'none','partitions':1,'security':'PLAINTEXT'}}
 def test_pass_needs_ack_bytes_keys_offsets_and_group_commit(self):
  value=self.receipt();checks.parse_receipt('ordinary client log\n'+json.dumps(value),'java')
  for field,bad in [('records',4),('settings',[]),('client_version','unknown'),('client','another client')]:
   altered=copy.deepcopy(value);altered[field]=bad
   with self.subTest(field=field),self.assertRaises(ValueError):checks.parse_receipt(json.dumps(altered),'java')
  for omitted in value['checks']:
   altered=copy.deepcopy(value);altered['checks'].remove(omitted)
   with self.subTest(omitted=omitted),self.assertRaises(ValueError):checks.parse_receipt(json.dumps(altered),'java')
 def test_duplicate_or_unstructured_success_is_rejected(self):
  text=json.dumps(self.receipt())
  for stdout in ['passed!',text+'\n'+text,'{}']:
   with self.subTest(stdout=stdout),self.assertRaises(ValueError):checks.parse_receipt(stdout,'java')
 def test_unsupported_settings_are_rejected(self):
  for key,bad in [('idempotence',True),('compression','gzip'),('partitions',2),('security','TLS')]:
   altered=self.receipt();altered['settings'][key]=bad
   with self.subTest(key=key),self.assertRaises(ValueError):checks.parse_receipt(json.dumps(altered),'java')
 def test_bundled_advertisements_match_source_without_invented_passes(self):
  data=json.loads((checks.ROOT/'docs/compatibility/compatibility.json').read_text())
  self.assertEqual(data['advertised_apis'],checks.advertised())
  self.assertEqual({i['language'] for i in data['clients']},set(checks.CLIENTS))
  self.assertTrue(all(i['status']=='unverified' for i in data['clients']))
if __name__=='__main__':unittest.main()
