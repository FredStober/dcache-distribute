import os, time

dCacheInfo = type('dCacheInfo', (),
	dict(map(lambda (idx, name): (name, idx + sum(map(ord, 'dCacheInfo'))), enumerate(
		['pfn', 'dcache_id', 'adler32', 'size', 'atime', 'storage_group', 'location']))))

def expath(fn):
	return os.path.realpath(os.path.expanduser(fn))

def progress(iter, step = 1000, enable = True, speed = True):
	if enable:
		t_start = time.time()
		try:
			idx = 0
			for idx, entry in enumerate(iter):
				if idx % step == 0:
					sys.stdout.write('\r%d' % idx)
					if speed:
						delta_t = float(time.time() - t_start)
						if delta_t > 2:
							sys.stdout.write(' %d/s' % (idx / delta_t))
					sys.stdout.flush()
				yield entry
			sys.stdout.write('\n')
			sys.stdout.flush()
		except:
			sys.stdout.write('\nError at %d\n' % idx)
			sys.stdout.flush()
			raise
	else:
		for entry in iter:
			yield entry


def get_cached(cache_file, fun, *args, **kwargs):
	import json
	if not os.path.exists(cache_file):
		result = fun(*args, **kwargs)
		json.dump(result, open(cache_file, 'w'))
	return json.load(open(cache_file))

# Get pool infos information
def get_pool_infos(dCacheWebHost):
	import xml.dom.minidom, webservice_api
	# Fixme: Use hostname cmsdcacheweb-kit.gridka.de instead of IP
	data = webservice_api.readURL('%s/info/pools' % dCacheWebHost)
	dom = xml.dom.minidom.parseString(data)
	dom_pools = dom.getElementsByTagName('dCache')[0].getElementsByTagName('pools')[0]
	for dom_pool in dom_pools.getElementsByTagName('pool'):
		result = {}
		result['name'] = str(dom_pool.attributes['name'].value)
		try:
			dom_poolgroups = dom_pool.getElementsByTagName('poolgroups')[0]
			for pg in dom_poolgroups.getElementsByTagName('poolgroupref'):
				result.setdefault('poolgroups', []).append(str(pg.attributes['name'].value))

			dom_space = dom_pool.getElementsByTagName('space')[0]
			for sm in dom_space.getElementsByTagName('metric'):
				result.setdefault('space', {})[str(sm.attributes['name'].value)] = float(sm.childNodes[0].data)
		except:
			print 'Unable to parse pool info', result['name']
		yield result

# Translate fn to dataset directory name - go to first non-numeric parent directory
def fn2ddn(fn):
	result = fn.rsplit('/', 1)[0]
	while True:
		try:
			tmp = result.rsplit('/', 1)
			int(tmp[1])
			result = tmp[0]
		except:
			break
	return result

# Filter chimera dump for moveable files
def filterMoveable(chimera_iter):
	for entry in chimera_iter:
		if dCacheInfo.dcache_id not in entry:
			continue
		if not entry.get(dCacheInfo.size, 0):
			continue
		if not entry.get(dCacheInfo.location, []):
			continue
		yield entry

# Create dCache script to perform transfers
def writeTransferCommands(fn, transfer_list):
	fp = open(fn, 'w')
	tmp = {}
	for (dcache_id, size, source, target) in transfer_list:
		tmp.setdefault(source, {}).setdefault(target, []).append(dcache_id)
	for source in sorted(tmp):
		fp.write('\\c %s\n' % source)
		for target in sorted(tmp[source]):
			fp.write('migration move -pnfsid=%s %s\n' % (str.join(',', sorted(tmp[source][target])), target))
		#fp.write('..\n') # "cd .." is wrong (as strange as it seems)
	fp.close()
	print 'Written result to', fp.name

# Read compressed chimera data
def get_chimera_data(fn):
	dn = None
	def fmtLoc(loc):
		loc = loc.strip()
		if not loc.startswith('f'):
			loc = 'osm:' + loc
		return loc
	fp = open(fn, 'r')
	for line in fp:
		if not line.strip():
			continue
		if line.startswith('/'):
			dn = line.strip()
		else:
			result = dict(zip([dCacheInfo.pfn, dCacheInfo.dcache_id, dCacheInfo.adler32,
				dCacheInfo.size, dCacheInfo.atime, dCacheInfo.location], line.split('\t')))
			result[dCacheInfo.pfn] = dn.rstrip('/') + '/' + result[dCacheInfo.pfn]
			result[dCacheInfo.size] = int(result[dCacheInfo.size])
			result[dCacheInfo.atime] = int(result[dCacheInfo.atime])
			if dCacheInfo.location in result:
				result[dCacheInfo.location] = map(fmtLoc, result.get(dCacheInfo.location, '').split(','))
			yield result
	fp.close()

log = logging.getLogger('webservice')

def user_agent(value):
	user_agent.value = value
user_agent.value = 'toolKIT/0.1'

def removeUnicode(obj):
	if type(obj) in (list, tuple, set):
		(obj, oldType) = (list(obj), type(obj))
		for i, v in enumerate(obj):
			obj[i] = removeUnicode(v)
		obj = oldType(obj)
	elif isinstance(obj, dict):
		result = {}
		for k, v in obj.iteritems():
			result[removeUnicode(k)] = removeUnicode(v)
		return result
	elif isinstance(obj, unicode):
		return str(obj)
	return obj

def readURL(url, params = None, headers = {}, cert = None):
	headers.setdefault('User-Agent', user_agent.value)

	import urllib, urllib2, httplib

	class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
		def __init__(self, key, cert):
			urllib2.HTTPSHandler.__init__(self)
			(self.key, self.cert) = (key, cert)
		def https_open(self, req):
			return self.do_open(self.getConnection, req)
		def getConnection(self, host, timeout = None):
			return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

	if cert:
		cert_handler = HTTPSClientAuthHandler(cert, cert)
		opener = urllib2.build_opener(cert_handler)
		urllib2.install_opener(opener)

	url_arg = None
	if params:
		url_arg = urllib.urlencode(params, doseq=True)
	log.info('Starting http query: %r %r' % (url, url_arg))
	log.debug('Connecting with header: %r' % headers)
	try:
		return urllib2.urlopen(urllib2.Request(url, url_arg, headers)).read()
	except:
		print 'Unable to open', url, 'with arguments', url_arg, 'and header', headers
		raise
