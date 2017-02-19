from django.shortcuts import render
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from Utilities import get_redis_connection0

redis_connection = get_redis_connection0()

feed_list = [redis_connection.hgetall(id) for id in redis_connection.smembers('WEBAPP_CACHE')]

# Create your views here.
def index(request):
	context = {'title': "News impacting Mexico-USA relations"}
	return render(request, 'newsapp/index.html', context)

def listing(request):
	paginator = Paginator(feed_list, 10) # Show 25 articles per page

	print paginator

	page = request.GET.get('page', 1)
	try:
		feeds = paginator.page(page)
	except PageNotAnInteger:
		# If page is not an integer, deliver first page.
		feeds = paginator.page(1)
	except EmptyPage:
		# If page is out of range (e.g. 9999), deliver last page of results.
		feeds = paginator.page(paginator.num_pages)
	return render(request, 'feed.html', {'feeds': feeds})
