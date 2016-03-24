
def get_cached(cache_file, fun, *args, **kwargs):
	import json
	if not os.path.exists(cache_file):
		result = fun(*args, **kwargs)
		json.dump(result, open(cache_file, 'w'))
	return json.load(open(cache_file))
