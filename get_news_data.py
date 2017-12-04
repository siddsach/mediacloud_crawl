import mediacloud
from datetime import datetime, date, timedelta
from boilerpipe.extract import Extractor
import os

SOURCE_PATH = 'source mediacloud IDS.csv'
OUT = 'data'

class Crawler:
    def __init__(self, api_key, source_path = SOURCE_PATH, particular_sources = None, max_stories = 100, out_path = OUT):
        self.mc = mediacloud.api.MediaCloud(api_key)
        self.sources = self.get_sources(source_path)
        self.keywords = []
        if particular_sources is not None:
            sources = {s:self.sources[s] for s in particular_sources}
            self.data = self.get_story_links(sources, max_stories)
        else:
            self.data = self.get_story_links(self.sources), max_stories
        self.get_articles(out_path)

    def get_sources(self, path):
        source_data = open(path, 'r').readlines()
        sources = dict()
        for line in source_data:
            parsed = line.strip('\n').split(',')
            sources[parsed[0]] = parsed[1]
        return sources

    def get_story_links(self, sources, start_date = None, end_date = None,days_to_subtract = 30):
        end_date = datetime.now()
        start_date = end_date - timedelta(days = days_to_subtract)
        date_query = self.mc.publish_date_query(start_date.date(), end_date.date())
        links = []
        for source in sources.keys():
            source_query = 'media_id:{}'.format(sources[source])
            print('Querying Mediacloud for stories from {} in the date range {}'.format(source, (start_date, end_date)))
            links += self.mc.storyList(solr_query = [source_query, date_query], rows = 100)
        return links

    def get_articles(self, out_path):
        os.mkdir(out_path)
        root = out_path
        sources = dict()
        for article in self.data:
            url = article['url']
            date = article['publish_date']
            title = article['title']
            source = article['media_name']
            text = Extractor(url = url).getText()
            if source not in os.listdir(out_path):
                sources[source] = 1
                os.makedirs(root + '/' + source)
            else:
                sources[source] += 1
            filename = str(sources[source]) + '.txt'
            f = open(root + '/' + source + "/" + filename, 'w')
            write_data = '\n'.join([url, date, title, source])
            write_data += '\n\n\nTEXT\n\n\n'
            write_data += text
            f.write(write_data)



