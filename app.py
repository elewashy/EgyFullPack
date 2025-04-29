from flask import Flask, render_template, request, url_for, abort, send_from_directory
import json
import math
import os

app = Flask(__name__)

PAGE_SIZE = 30

def load_series_qualities(series_id):
    series_dir = os.path.join("data/cimanow/ar-series/ids", str(series_id))
    if not os.path.exists(series_dir):
        return None
        
    summary_file = os.path.join(series_dir, "summary.json")
    if not os.path.exists(summary_file):
        return None
        
    with open(summary_file, "r", encoding="utf-8") as f:
        return json.load(f)

def load_quality_links(series_id, source, quality):
    quality_file = os.path.join("data/cimanow/ar-series/ids", str(series_id), f"{source}_{quality}.json")
    if not os.path.exists(quality_file):
        return None
        
    with open(quality_file, "r", encoding="utf-8") as f:
        return json.load(f)

@app.route("/")
def series():
    page = int(request.args.get("page", 1))
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
    
    # Add VK JSON links
    if "vk" in qualities_info["qualities"]:
        for quality in qualities_info["qualities"]["vk"]:
            json_links.append({
                "server": "VK.com",
                "quality": quality,
                "url": f"/data/cimanow/ar-series/ids/{series_id}/vk_{quality}.json"
            })
                    
    # Add Deva JSON links
    if "deva" in qualities_info["qualities"]:
        for quality in qualities_info["qualities"]["deva"]:
            json_links.append({
                "server": "إِيجي فيلم",
                "quality": quality,
                "url": f"/data/cimanow/ar-series/ids/{series_id}/deva_{quality}.json"
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

if __name__ == "__main__":
    app.run(debug=True)
