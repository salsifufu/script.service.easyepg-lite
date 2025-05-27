from datetime import datetime, timedelta
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache # Importação adicionada para caching

# --- Constantes Globais ---
TMS_API_BASE_URL = "http://data.tmsapi.com/v1.1"
GOOGLE_TRANSLATE_API_URL = "https://translate.googleapis.com/translate_a/single"
MAX_CONCURRENT_REQUESTS_TMS = 10 
MAX_CONCURRENT_REQUESTS_GOOGLE_TRANSLATE = 5 

# --- Funções de Autenticação e Geração de Links ---

def login(data, credentials, headers):
    new_key = credentials["key"]
    url = f"{TMS_API_BASE_URL}/stations/10359?lineupId=USA-TX42500-X&api_key={new_key}"
    try:
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        response.json()
        return True, {"key": new_key}
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        return False, {"message": "Invalid key or API error"}
    
def epg_main_links(data, channels, settings, session, headers):
    time_start = datetime.now().strftime("%Y-%m-%dT06:00Z")
    time_end = (datetime.now() + timedelta(days=int(settings["days"]))).strftime("%Y-%m-%dT06:00Z")
    
    return [{"url": f"{TMS_API_BASE_URL}/stations/{c}/airings?startDateTime={time_start}&endDateTime={time_end}&imageSize={settings['is']}&imageAspectTV={settings['it']}&api_key={session['session']['key']}",
             "h": headers, "c": c} for c in channels]

# --- Funções de Busca e Conversão de Dados EPG ---

def fetch_epg_data_for_channel(url_info):
    url = url_info["url"]
    headers = url_info["h"]
    channel_id = url_info["c"]
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json(), channel_id
    except requests.exceptions.RequestException as e:
        return None, channel_id

@lru_cache(maxsize=512) # Caching adicionado aqui
def translate_text_google(text, target_language="pt", source_language="auto"):
    if not text:
        return text

    params = {
        "client": "gtx",
        "sl": source_language,
        "tl": target_language,
        "dt": "t",
        "q": text
    }
    
    try:
        response = requests.get(GOOGLE_TRANSLATE_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data and isinstance(data, list) and data[0] and isinstance(data[0], list) and data[0][0]:
            return data[0][0][0]
        
    except (requests.exceptions.Timeout, requests.exceptions.RequestException, json.JSONDecodeError) as e:
        pass
    return text

def epg_main_converter(data_json_str, channels, settings, ch_id=None):
    item = json.loads(data_json_str)
    airings = []
    
    descriptions_to_translate = []
    original_text_map = {} 

    for idx, i in enumerate(item):
        program = i["program"]
        original_long_desc = program.get("longDescription")
        original_short_desc = program.get("shortDescription")
        desc_to_process = original_long_desc or original_short_desc

        source_language_full = program.get("descriptionLang")
        source_language_short = source_language_full.split('-')[0].lower() if source_language_full else None
        target_language_code = "pt"

        if desc_to_process and source_language_short != target_language_code:
            descriptions_to_translate.append({
                "index": idx,
                "text": desc_to_process,
                "target_language": target_language_code,
                "source_language": source_language_short
            })
            original_text_map[idx] = desc_to_process 
        else:
            g = {} 
            g["desc"] = desc_to_process
            item[idx]['_temp_g_desc'] = g["desc"] 

    translated_results = {}
    if descriptions_to_translate:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS_GOOGLE_TRANSLATE) as executor:
            future_to_desc = {
                executor.submit(translate_text_google, d["text"], d["target_language"], d["source_language"]): d["index"]
                for d in descriptions_to_translate
            }
            for future in as_completed(future_to_desc):
                idx = future_to_desc[future]
                try:
                    translated_text = future.result()
                    translated_results[idx] = translated_text
                except Exception as exc:
                    translated_results[idx] = original_text_map.get(idx, "") 

    for idx, i in enumerate(item):
        program, g = i["program"], {}
        g["c_id"] = ch_id
        g["start"] = int(datetime.strptime(i["startTime"], "%Y-%m-%dT%H:%MZ").timestamp())
        g["end"] = int(datetime.strptime(i["endTime"], "%Y-%m-%dT%H:%MZ").timestamp())
        g["b_id"] = f'{program["tmsId"]}_{g["start"]}_{g["end"]}_{g["c_id"]}'

        entity_type = program.get("entityType", "None")
        qualifiers = i.get("qualifiers", [])
        
        title_string = program["title"]
        subtitle = program.get("episodeTitle") or program.get("eventTitle")
        
        if subtitle and entity_type == "Sports": title_string = f"{title_string}: {subtitle}"
        if "Live" in qualifiers: title_string = f"{title_string} - Ao Vivo"
        if ch_id not in ["105501", "103758"] and entity_type == "Sports" and "Live" not in qualifiers: title_string = f"VT - {title_string}"
        g["title"] = title_string

        if subtitle and entity_type != "Sports": g["subtitle"] = subtitle
        g["image"] = program.get("preferredImage", {}).get("uri")
        
        g["desc"] = translated_results.get(idx, i.get('_temp_g_desc'))
        g["desc"] = g["desc"] if g["desc"] is not None else ""
            
        if "gameDate" in program and entity_type == "Sports" and "Live" not in qualifiers:
            try:
                formatted_date = datetime.strptime(program["gameDate"], "%Y-%m-%d").strftime("%d/%m/%Y")
            except ValueError:
                formatted_date = program["gameDate"]
            g["desc"] = f"{g['desc']}\n\nEvento realizado em: {formatted_date}"
            
        g["date"] = (datetime.strptime(program["origAirDate"], "%Y-%m-%d").strftime("%Y") if program.get("origAirDate") else None) or \
                     (str(program["releaseYear"]) if program.get("releaseYear") else None)

        star = program.get("qualityRating", {}).get("value")
        if star is not None: g["star"] = {"system": "TMS", "value": f"{star}/4"}

        g["director"] = program.get("directors", [])
        g["actor"] = program.get("topCast", [])
        g["credits"] = {"director": g["director"], "actor": g["actor"]}

        g["season_episode_num"] = {"season": program.get("seasonNum"), "episode": program.get("episodeNum")}

        g["genres"] = program.get("genres", [])
        if entity_type in ["Sports", "Movie"]: g["genres"].append(entity_type)
        if qualifiers: g["qualifiers"] = qualifiers

        rating_info = next((r for r in program.get("ratings", [])
                            if r["body"] == "Departamento de Justiça, Classificação, Títulos e Qualificação" and settings["at"] == "BRA"), None)
        g["rating"] = {"system": settings["at"].upper(), "value": rating_info["code"]} if rating_info else {"system": None, "value": None}
            
        airings.append(g)

    return airings

# --- Função Principal de Geração de EPG ---

def generate_full_epg(data, channels, settings, session, headers):
    url_list = epg_main_links(data, channels, settings, session, headers)

    all_airings = []
    
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS_TMS) as executor:
        future_to_url_info = {executor.submit(fetch_epg_data_for_channel, url_info): url_info for url_info in url_list}
        
        for future in as_completed(future_to_url_info):
            url_info = future_to_url_info[future]
            channel_id = url_info["c"]
            try:
                data_json, fetched_channel_id = future.result()
                if data_json:
                    processed_data = epg_main_converter(json.dumps(data_json), channels, settings, ch_id=fetched_channel_id)
                    all_airings.extend(processed_data)
                else:
                    pass
            except Exception as exc:
                pass
    
    return all_airings