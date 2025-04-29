import json
import os
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('series_downloader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class SeriesDownloader:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.ids_dir = os.path.join(self.script_dir, 'ids')
        self.progress_file = os.path.join(self.script_dir, 'progress.json')
        self.processed_data_file = os.path.join(self.script_dir, 'processed_data.json')
        self.session = None
        self.progress_data = self.load_progress()
        self.processed_data = self.load_processed_data()
        
    def load_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Error loading progress file: {e}")
        return {
            'completed_series': [],
            'completed_episodes': {},
            'last_update': {}
        }

    def load_processed_data(self):
        if os.path.exists(self.processed_data_file):
            try:
                with open(self.processed_data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Error loading processed data: {e}")
        return {}

    def save_progress(self):
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.progress_data, f, ensure_ascii=False, indent=2)

    def save_processed_data(self):
        with open(self.processed_data_file, 'w', encoding='utf-8') as f:
            json.dump(self.processed_data, f, ensure_ascii=False, indent=2)

    def get_series_dir(self, series_id):
        series_dir = os.path.join(self.ids_dir, str(series_id))
        os.makedirs(series_dir, exist_ok=True)
        return series_dir

    async def fetch(self, url, retries=3):
        for attempt in range(retries):
            try:
                async with self.session.get(url, ssl=False, timeout=30) as response:
                    if response.status == 200:
                        return await response.text()
                    logging.warning(f"HTTP {response.status} for {url}")
            except Exception as e:
                if attempt == retries - 1:
                    logging.error(f"Failed to fetch {url}: {e}")
                    raise
                await asyncio.sleep(1)

    async def get_episode_links(self, url):
        try:
            html = await self.fetch(url)
            soup = BeautifulSoup(html, 'html.parser')
            episodes = soup.select('ul.tabcontent.active#eps li a')
            return [(ep.select_one('em').text, ep['href']) for ep in episodes]
        except Exception as e:
            logging.error(f"Error getting episodes from {url}: {e}")
            return []

    def process_url(self, url):
        return url.replace('drone.worldcdn.online', 'deva-cpmav9sk6x41.cimanowtv.com')

    async def get_download_links(self, episode_url):
        try:
            watching_url = episode_url + 'watching/'
            html = await self.fetch(watching_url)
            soup = BeautifulSoup(html, 'html.parser')
            
            quality_links = {'vk': {}, 'deva': {}}
            
            for quality_box in soup.select('li[aria-label="quality"]'):
                for link in quality_box.select('a'):
                    quality = link.text.strip().split('\n')[0].strip()
                    url = self.process_url(link['href'])
                    size = link.select_one('p').text.strip()
                    
                    if 'vk.com' in url:
                        quality_links['vk'][quality] = {'url': url, 'size': size}
                    else:
                        quality_links['deva'][quality] = {'url': url, 'size': size}
            
            return quality_links
        except Exception as e:
            logging.error(f"Error processing {episode_url}: {e}")
            return {'vk': {}, 'deva': {}}

    def save_quality_file(self, series, ep_num, source, quality, data, series_dir):
        quality_file = os.path.join(series_dir, f'{source}_{quality}.json')
        
        try:
            quality_content = {
                'title': series['name'],
                'episodes': []
            }
            
            if os.path.exists(quality_file):
                with open(quality_file, 'r', encoding='utf-8') as f:
                    quality_content = json.load(f)
            
            # Add new episode if not exists
            ep_exists = any(ep['name'] == f'Episode {ep_num}' for ep in quality_content['episodes'])
            
            if not ep_exists:
                quality_content['episodes'].append({
                    'name': f'Episode {ep_num}',
                    'url': data['url'],
                    'size': data['size']
                })
                
                quality_content['episodes'].sort(key=lambda x: int(x['name'].split()[-1]))
                
                with open(quality_file, 'w', encoding='utf-8') as f:
                    json.dump(quality_content, f, ensure_ascii=False, indent=2)
                
                return True  # New episode was added
            
            return False  # Episode already existed
            
        except Exception as e:
            logging.error(f"Error saving quality file for episode {ep_num}: {e}")
            return False

    async def process_episode(self, series, ep_num, ep_url, series_dir):
        try:
            quality_links = await self.get_download_links(ep_url)
            
            new_content_added = False
            for source, links in quality_links.items():
                for quality, data in links.items():
                    if self.save_quality_file(series, ep_num, source, quality, data, series_dir):
                        new_content_added = True
            
            return new_content_added
            
        except Exception as e:
            logging.error(f"Error processing episode {ep_num}: {e}")
            return False

    def create_series_summary(self, series, series_dir):
        try:
            summary = {'title': series['name'], 'qualities': {}}
            for source in ['vk', 'deva']:
                for quality_file in os.listdir(series_dir):
                    if quality_file.startswith(f'{source}_') and quality_file.endswith('.json'):
                        quality = quality_file.replace(f'{source}_', '').replace('.json', '')
                        if source not in summary['qualities']:
                            summary['qualities'][source] = {}
                        summary['qualities'][source][quality] = quality_file

            with open(os.path.join(series_dir, 'summary.json'), 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logging.error(f"Error creating summary for {series['name']}: {e}")

    def validate_series_completion(self, series, series_dir, all_episodes):
        try:
            # Check each quality file
            found_episodes = set()
            for fname in os.listdir(series_dir):
                if fname.endswith('.json') and fname != 'summary.json':
                    with open(os.path.join(series_dir, fname), 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for ep in data.get('episodes', []):
                            if ep.get('name', '').startswith('Episode '):
                                ep_num = ep['name'].replace('Episode ', '').strip()
                                found_episodes.add(ep_num)

            # Compare with all expected episodes
            all_ep_nums = set(ep_num for ep_num, _ in all_episodes)
            missing = sorted(list(all_ep_nums - found_episodes), key=lambda x: int(x))
            
            return len(missing) == 0, missing
            
        except Exception as e:
            logging.error(f"Error validating series completion: {e}")
            return False, []

    async def process_series(self, series):
        series_id = str(series['id'])
        series_dir = self.get_series_dir(series_id)
        
        try:
            # Get current episodes
            current_episodes = await self.get_episode_links(series['link'])
            if not current_episodes:
                logging.error(f"No episodes found for {series['name']}")
                return
            
            # Get previously processed episodes
            prev_episodes = self.processed_data.get(series_id, {'episodes': []})
            
            # Find new episodes
            prev_ep_nums = set(ep['number'] for ep in prev_episodes.get('episodes', []))
            new_episodes = [(num, url) for num, url in current_episodes 
                          if num not in prev_ep_nums]
            
            if new_episodes:
                logging.info(f"Found {len(new_episodes)} new episodes for {series['name']}")
                
                # Process new episodes sequentially
                for ep_num, ep_url in new_episodes:
                    new_content = await self.process_episode(series, ep_num, ep_url, series_dir)
                    if new_content:
                        # Update processed data
                        if series_id not in self.processed_data:
                            self.processed_data[series_id] = {'episodes': []}
                        
                        self.processed_data[series_id]['episodes'].append({
                            'number': ep_num,
                            'processed_at': datetime.now().isoformat()
                        })
                        
                        # Save after each episode to prevent data loss
                        self.save_processed_data()
                
                # Create/update series summary
                self.create_series_summary(series, series_dir)
                
                # Validate completion
                is_complete, missing = self.validate_series_completion(
                    series, series_dir, current_episodes)
                
                if is_complete:
                    logging.info(f"Successfully completed series: {series['name']}")
                else:
                    logging.warning(
                        f"Series {series['name']} is incomplete. Missing episodes: {missing}")
                    
                # Update progress data
                self.progress_data['last_update'][series_id] = datetime.now().isoformat()
                if is_complete and series_id not in self.progress_data['completed_series']:
                    self.progress_data['completed_series'].append(series_id)
                self.save_progress()
                    
            else:
                logging.info(f"No new episodes found for {series['name']}")
                
        except Exception as e:
            logging.error(f"Error processing series {series['name']}: {e}")

    async def run(self):
        # Load series list
        series_file = os.path.join(self.script_dir, 'ar-series.json')
        try:
            with open(series_file, 'r', encoding='utf-8') as f:
                series_list = json.load(f)
        except Exception as e:
            logging.error(f"Error loading series list: {e}")
            return

        # Create ids directory if it doesn't exist
        os.makedirs(self.ids_dir, exist_ok=True)

        # Process series with limited concurrency
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session
            
            # Process series one at a time to avoid overwhelming the server
            for series in series_list:
                await self.process_series(series)

def main():
    downloader = SeriesDownloader()
    asyncio.run(downloader.run())

if __name__ == '__main__':
    main()