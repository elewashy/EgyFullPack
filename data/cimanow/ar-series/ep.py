import json
import os
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import time

async def fetch(session, url):
    async with session.get(url, ssl=False) as response:
        return await response.text()

async def get_episode_links(session, url):
    try:
        html = await fetch(session, url)
        soup = BeautifulSoup(html, 'html.parser')
        episodes = soup.select('ul.tabcontent.active#eps li a')
        return [(ep.select_one('em').text, ep['href']) for ep in episodes]
    except Exception as e:
        print(f"Error getting episodes from {url}: {str(e)}")
        return []

def process_url(url):
    if 'drone.worldcdn.online' in url:
        return url.replace('drone.worldcdn.online', 'deva-cpmav9sk6x41.cimanowtv.com')
    return url

async def get_download_links(session, episode_url):
    try:
        watching_url = episode_url + 'watching/'
        html = await fetch(session, watching_url)
        soup = BeautifulSoup(html, 'html.parser')
        
        quality_links = {'vk': {}, 'deva': {}}
        
        # Process all quality boxes
        for quality_box in soup.select('li[aria-label="quality"]'):
            # Process each link in the quality box
            for link in quality_box.select('a'):
                quality = link.text.strip().split('\n')[0].strip()
                url = process_url(link['href'])
                size = link.select_one('p').text.strip()
                
                # Determine source based on URL
                if 'vk.com' in url:
                    quality_links['vk'][quality] = {'url': url, 'size': size}
                else:
                    # All non-VK links go to deva category
                    quality_links['deva'][quality] = {'url': url, 'size': size}
        
        return quality_links
    except Exception as e:
        print(f"Error processing {episode_url}: {str(e)}")
        return {'vk': {}, 'deva': {}}

async def save_quality_file(series, ep_num, source, quality, data, series_dir):
    quality_file = os.path.join(series_dir, f'{source}_{quality}.json')
    
    # Create or load quality file content
    quality_content = {
        'title': series['name'],
        'episodes': []
    }
    
    if os.path.exists(quality_file):
        try:
            with open(quality_file, 'r', encoding='utf-8') as f:
                quality_content = json.load(f)
        except:
            pass
    
    # Add new episode if not already exists
    ep_exists = False
    for ep in quality_content['episodes']:
        if ep['name'] == f'Episode {ep_num}':
            ep_exists = True
            break
            
    if not ep_exists:
        quality_content['episodes'].append({
            'name': f'Episode {ep_num}',
            'url': data['url'],
            'size': data['size']
        })
        
        # Sort episodes by number
        quality_content['episodes'].sort(key=lambda x: int(x['name'].split()[-1]))
        
        # Save updated quality content
        with open(quality_file, 'w', encoding='utf-8') as f:
            json.dump(quality_content, f, ensure_ascii=False, indent=2)

async def process_episode(session, series, ep_num, ep_url, series_dir):
    print(f"Processing {series['name']} Episode {ep_num}")
    quality_links = await get_download_links(session, ep_url)
    
    tasks = []
    for source, links in quality_links.items():
        for quality, data in links.items():
            task = save_quality_file(series, ep_num, source, quality, data, series_dir)
            tasks.append(task)
    
    if tasks:
        await asyncio.gather(*tasks)

def create_summary(series, series_dir):
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

def load_progress(script_dir):
    progress_file = os.path.join(script_dir, 'progress.json')
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {
        'completed_series': [],
        'current_series': None,
        'completed_episodes': {},
        'failed_episodes': {}
    }

def save_progress(script_dir, progress_data):
    progress_file = os.path.join(script_dir, 'progress.json')
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=2)

async def process_series(session, series, script_dir, progress_data):
    series_id = series['id']
    series_link = series['link']
    
    # Skip if series already completed
    if series_id in progress_data['completed_series']:
        print(f"Skipping completed series {series['name']} (ID: {series_id})")
        return

    # Create directory for this series ID
    series_dir = os.path.join(script_dir, 'ids', str(series_id))
    os.makedirs(series_dir, exist_ok=True)
    
    # Update current series in progress
    progress_data['current_series'] = series_id
    save_progress(script_dir, progress_data)
    
    # Get episodes for this series
    episodes = await get_episode_links(session, series_link)
    
    # Initialize completed episodes for this series if not exists
    if str(series_id) not in progress_data['completed_episodes']:
        progress_data['completed_episodes'][str(series_id)] = []
    
    if str(series_id) not in progress_data['failed_episodes']:
        progress_data['failed_episodes'][str(series_id)] = []
    
    # Process episodes concurrently in batches
    batch_size = 20  # Process 20 episodes at a time for higher speed
    max_retries = 3

    for i in range(0, len(episodes), batch_size):
        batch = episodes[i:i + batch_size]
        tasks = []
        batch_ep_nums = []
        for ep_num, ep_url in batch:
            # Skip if episode already completed
            if ep_num in progress_data['completed_episodes'][str(series_id)]:
                print(f"Skipping completed episode {ep_num} of {series['name']}")
                continue

            batch_ep_nums.append(ep_num)
            tasks.append((ep_num, ep_url))

        # Retry logic for each episode in the batch
        for ep_num, ep_url in tasks:
            success = False
            for attempt in range(max_retries):
                try:
                    await process_episode(session, series, ep_num, ep_url, series_dir)
                    if ep_num not in progress_data['completed_episodes'][str(series_id)]:
                        progress_data['completed_episodes'][str(series_id)].append(ep_num)
                    save_progress(script_dir, progress_data)
                    success = True
                    break
                except Exception as e:
                    print(f"Error processing episode {ep_num} (attempt {attempt+1}): {str(e)}")
                    await asyncio.sleep(0.5)
            if not success:
                if ep_num not in progress_data['failed_episodes'][str(series_id)]:
                    progress_data['failed_episodes'][str(series_id)].append(ep_num)
                save_progress(script_dir, progress_data)

        # Small delay between batches to be nice to server
        if i + batch_size < len(episodes):
            await asyncio.sleep(0.5)
    
    create_summary(series, series_dir)
    
    # Mark series as completed
    progress_data['completed_series'].append(series_id)
    progress_data['current_series'] = None
    save_progress(script_dir, progress_data)
    
    # Check for missing episodes (progress.json)
    all_ep_nums = set(ep_num for ep_num, _ in episodes)
    completed_ep_nums = set(progress_data['completed_episodes'][str(series_id)])
    missing_ep_nums = sorted(list(all_ep_nums - completed_ep_nums), key=lambda x: int(x))
    if missing_ep_nums:
        print(f'WARNING: Missing episodes for {series["name"]} (ID: {series_id}) in progress.json: {missing_ep_nums}')
    else:
        print(f'All episodes completed for {series["name"]} (ID: {series_id}) in progress.json')

    # Check for missing episodes in JSON files
    found_ep_nums = set()
    for fname in os.listdir(series_dir):
        if fname.endswith('.json') and fname not in ['summary.json']:
            try:
                with open(os.path.join(series_dir, fname), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for ep in data.get('episodes', []):
                        ep_name = ep.get('name', '')
                        if ep_name.startswith('Episode '):
                            ep_num = ep_name.replace('Episode ', '').strip()
                            found_ep_nums.add(ep_num)
            except Exception as e:
                print(f'Error reading {fname}: {e}')
    missing_in_json = sorted(list(all_ep_nums - found_ep_nums), key=lambda x: int(x))
    if missing_in_json:
        print(f'WARNING: Missing episodes for {series["name"]} (ID: {series_id}) in JSON files: {missing_in_json}')
    else:
        print(f'All episodes present in JSON files for {series["name"]} (ID: {series_id})')

    print(f'Completed series {series["name"]} (ID: {series_id})')

async def retry_failed_episodes(session, script_dir, progress_data, series_list):
    for series in series_list:
        series_id = str(series['id'])
        if series_id in progress_data['failed_episodes'] and progress_data['failed_episodes'][series_id]:
            print(f"\nRetrying failed episodes for series {series['name']}")
            failed_eps = progress_data['failed_episodes'][series_id]
            progress_data['failed_episodes'][series_id] = []
            save_progress(script_dir, progress_data)
            
            # Get episodes again
            episodes = await get_episode_links(session, series['link'])
            
            # Filter only failed episodes
            failed_episodes = [(ep_num, ep_url) for ep_num, ep_url in episodes if ep_num in failed_eps]
            
            # Process failed episodes
            for ep_num, ep_url in failed_episodes:
                try:
                    await process_episode(session, series, ep_num, ep_url, os.path.join(script_dir, 'ids', series_id))
                    if str(series_id) not in progress_data['completed_episodes']:
                        progress_data['completed_episodes'][str(series_id)] = []
                    progress_data['completed_episodes'][str(series_id)].append(ep_num)
                except Exception as e:
                    print(f"Failed to process episode {ep_num}: {str(e)}")
                    if ep_num not in progress_data['failed_episodes'][series_id]:
                        progress_data['failed_episodes'][series_id].append(ep_num)
                save_progress(script_dir, progress_data)

async def main():
    # Get the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Load progress
    progress_data = load_progress(script_dir)
    
    # Open ar-series.json from script directory
    json_path = os.path.join(script_dir, 'ar-series.json')
    with open(json_path, 'r', encoding='utf-8') as f:
        series_list = json.load(f)

    # Process multiple series concurrently
    connector = aiohttp.TCPConnector(limit=50)  # Increase concurrency for speed
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for series in series_list:
            task = process_series(session, series, script_dir, progress_data)
            tasks.append(task)
        await asyncio.gather(*tasks)

        # Retry failed episodes
        await retry_failed_episodes(session, script_dir, progress_data, series_list)

if __name__ == '__main__':
    asyncio.run(main())
