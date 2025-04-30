from flask import Flask, render_template, request, url_for, abort, send_from_directory, jsonify, redirect
import json
import math
import os

app = Flask(__name__)

PAGE_SIZE = 30

def get_pagination(page, total_pages):
    pagination = []
    
    def add_page(i):
        pagination.append({
            "number": i,
            "url": request.path + f"?q={request.args.get('q', '')}&page={i}&view=page",
            "active": (i == page)
        })
    
    def add_ellipsis():
        pagination.append({
            "number": "...",
            "url": "javascript: void(0)",
            "active": False
        })

    # Always show first page
    add_page(1)
    
    # Show pages around current page
    for i in range(max(2, page - 2), min(total_pages, page + 3)):
        if i == 2 and page > 4:
            add_ellipsis()
        add_page(i)
            
    # Show last page with ellipsis if needed
    if page + 2 < total_pages - 1:
        add_ellipsis()
    if page + 2 < total_pages:
        add_page(total_pages)
        
    return pagination

def load_series_qualities(series_id):
    series_dir = os.path.join("data/cimanow/ar-series/ids", str(series_id))
    if not os.path.exists(series_dir):
        return None
    
    qualities = {"qualities": {}}
    # Check for VK qualities
    for file in os.listdir(series_dir):
        if file.startswith("vk_") and file.endswith(".json"):
            if "vk" not in qualities["qualities"]:
                qualities["qualities"]["vk"] = []
            quality = file[3:-5]  # Remove vk_ prefix and .json suffix
            qualities["qualities"]["vk"].append(quality)
            
    # Check for Deva qualities
    for file in os.listdir(series_dir):
        if file.startswith("deva_") and file.endswith(".json"):
            if "deva" not in qualities["qualities"]:
                qualities["qualities"]["deva"] = []
            quality = file[5:-5]  # Remove deva_ prefix and .json suffix
            qualities["qualities"]["deva"].append(quality)
            
    return qualities if qualities["qualities"] else None


def load_quality_links(series_id, source, quality):
    quality_file = os.path.join("data/cimanow/ar-series/ids", str(series_id), f"{source}_{quality}.json")
    if not os.path.exists(quality_file):
        return None
        
    with open(quality_file, "r", encoding="utf-8") as f:
        return json.load(f)

def get_series_data():
    with open("data/cimanow/ar-series/ar-series.json", "r", encoding="utf-8") as f:
        series_list = json.load(f)
    series_data = []
    for s in series_list:
        series_data.append({
            "id": s.get("id", ""),
            "title": s.get("name", ""),
            "title_ar": s.get("title_ar", ""),
            "quality": s.get("ribbon", [""])[0] if isinstance(s.get("ribbon", ""), list) and s.get("ribbon", []) else "",
            "ribbon": s.get("ribbon", []),
            "genres": [g.strip() for g in s.get("genre", "").split("،") if g.strip()],
            "categories": s.get("season", ""),
            "image": s.get("image", ""),
            "link": url_for("download", series_id=s.get("id", ""))
        })
    return series_data

@app.route("/search")
def search():
    query = request.args.get("q", "").strip()
    view = request.args.get("view", "json")
    
    if not query:
        if view == "page":
            return redirect(url_for("series"))
        return jsonify({"results": []})
    
    series_data = get_series_data()
    query_lower = query.lower()
    
    # Search in titles
    results = [
        s for s in series_data 
        if query_lower in s["title"].lower() or 
           query_lower in s["title_ar"].lower()
    ]
    
    if view == "page":
        # Return full page view
        page = int(request.args.get("page", 1))
        total = len(results)
        total_pages = math.ceil(total / PAGE_SIZE)
        start = (page - 1) * PAGE_SIZE
        end = start + PAGE_SIZE
        series_page = results[start:end]
        
        pagination = get_pagination(page, total_pages)
        
        return render_template(
            "series.html", 
            series=series_page, 
            pagination=pagination, 
            page=page, 
            total_pages=total_pages,
            search_query=query
        )
    
    # Return JSON results for live search
    return jsonify({"results": results[:10]})  # Limit to 10 results for dropdown

@app.route("/")
def series():
    page = int(request.args.get("page", 1))
    series_data = get_series_data()
    total = len(series_data)
    total_pages = math.ceil(total / PAGE_SIZE)
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    series_page = series_data[start:end]
    pagination = []
    def add_page(i):
        pagination.append({
            "number": i,
            "url": url_for("series", page=i),
            "active": (i == page)
        })
    
    def add_ellipsis():
        pagination.append({
            "number": "...",
            "url": "#",
            "active": False
        })

    # Always show first page
    add_page(1)
    
    # Show pages around current page
    for i in range(max(2, page - 2), min(total_pages, page + 3)):
        if i == 2 and page > 4:
            add_ellipsis()
        add_page(i)
            
    # Show last page with ellipsis if needed
    if page + 2 < total_pages - 1:
        add_ellipsis()
    if page + 2 < total_pages:
        add_page(total_pages)
    return render_template("series.html", series=series_page, pagination=pagination, page=page, total_pages=total_pages)

@app.route("/series/<series_id>/download")
def download(series_id):
    # Load series info
    with open("data/cimanow/ar-series/ar-series.json", "r", encoding="utf-8") as f:
        series_list = json.load(f)
    series = next((s for s in series_list if str(s.get("id")) == str(series_id)), None)
    if not series:
        abort(404)
        
    # Load qualities info
    qualities_info = load_series_qualities(series_id)
    if not qualities_info:
        abort(404)
        
    # Prepare JSON file links
    json_links = []
    
    def sort_quality(q):
        # Convert quality string to numeric value for sorting
        try:
            return int(''.join(filter(str.isdigit, q)))
        except:
            return 0
            
    # Add Deva (EgyFilm) JSON links first
    if "deva" in qualities_info["qualities"]:
        sorted_qualities = sorted(qualities_info["qualities"]["deva"], key=sort_quality, reverse=True)
        for quality in sorted_qualities:
            json_links.append({
                "server": "EgyFilm",
                "quality": quality,
                "url": f"/data/cimanow/ar-series/ids/{series_id}/deva_{quality}.json"
            })
                    
    # Add VK JSON links second
    if "vk" in qualities_info["qualities"]:
        sorted_qualities = sorted(qualities_info["qualities"]["vk"], key=sort_quality, reverse=True)
        for quality in sorted_qualities:
            json_links.append({
                "server": "VK.com",
                "quality": quality,
                "url": f"/data/cimanow/ar-series/ids/{series_id}/vk_{quality}.json"
            })
    

    
    return render_template(
        "download.html",
        series={
            "id": series_id,
            "title": series.get("title_ar", ""),
            "download_links": json_links,
            "description": series.get("genre", ""),
            "season": series.get("season", "")
        }
    )

# Serve static JSON files
@app.route('/data/cimanow/ar-series/ids/<series_id>/<filename>')
def serve_json(series_id, filename):
    directory = f"data/cimanow/ar-series/ids/{series_id}"
    return send_from_directory(directory, filename, as_attachment=True)

# Allow serving static files in debug mode
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

if __name__ == "__main__":
    app.run(debug=True)
