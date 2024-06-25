import sqlite3
import json
import os

input_db = 'crawl-data.sqlite'
output_db = 'resultado.sqlite'
storage_json = 'storage.json'

storage_data = {}
if os.path.exists(storage_json):
    with open(storage_json, 'r') as file:
        storage_data = json.load(file)

conn_input = sqlite3.connect(input_db)
cursor_input = conn_input.cursor()

conn_output = sqlite3.connect(output_db)
cursor_output = conn_output.cursor()

# Crea las tablas
cursor_output.execute('''
    CREATE TABLE IF NOT EXISTS stateful (
        site_url TEXT,
        session_and_first_party_cookies INTEGER,
        persistent_and_first_party_cookies INTEGER,
        session_and_third_party_cookies INTEGER,
        persistent_and_third_party_cookies INTEGER,
        session_storage INTEGER,
        persistent_storage INTEGER
    )
''')

cursor_output.execute('''
    CREATE TABLE IF NOT EXISTS stateless (
        site_url TEXT,
        canvas_fingerprinting INTEGER,
        webgl_fingerprinting INTEGER,
        webrtc_fingerprinting INTEGER,
        audio_fingerprinting INTEGER
    )
''')

cursor_output.execute('''
    CREATE TABLE IF NOT EXISTS trackers (
        site_url TEXT,
        doubleclick INTEGER,
        google_analytics INTEGER,
        google_syndication INTEGER,
        facebook INTEGER,
        twitter INTEGER,
        criteo INTEGER
    )
''')

cursor_input.execute('''
    SELECT sv.site_url
    FROM 
        site_visits sv 
    WHERE (
        sv.visit_id IN (
            SELECT 
                visit_id 
            FROM 
                http_responses 
            WHERE response_status LIKE '2%'
        )
    ) 
    GROUP BY 
        sv.site_url;
''')
valid_site_urls = {row[0] for row in cursor_input.fetchall()}

############## Stateful #############
cursor_input.execute('''
    SELECT 
        sv.site_url,
        COUNT(DISTINCT CASE WHEN jc.is_session = 1 AND (jc.host LIKE '%.' || REPLACE(sv.site_url, 'http://', '')) THEN jc.name END) AS session_and_first_party_cookies,
        COUNT(DISTINCT CASE WHEN jc.is_session = 0 AND (jc.host LIKE '%.' || REPLACE(sv.site_url, 'http://', '')) THEN jc.name END) AS persistent_and_first_party_cookies,
        COUNT(DISTINCT CASE WHEN jc.is_session = 1 AND (jc.host NOT LIKE '%.' || REPLACE(sv.site_url, 'http://', '')) THEN jc.name END) AS session_and_third_party_cookies,
        COUNT(DISTINCT CASE WHEN jc.is_session = 0 AND (jc.host NOT LIKE '%.' || REPLACE(sv.site_url, 'http://', '')) THEN jc.name END) AS persistent_and_third_party_cookies
    FROM 
        site_visits sv
    LEFT JOIN 
        javascript_cookies jc ON sv.visit_id = jc.visit_id
    GROUP BY 
        sv.site_url;
''')

results = cursor_input.fetchall()

filtered_results = []
for row in results:
    site_url = row[0]
    if site_url in valid_site_urls:
        filtered_results.append(row)

results_with_storage = []
for row in filtered_results:
    site_url = row[0]
    session_storage = storage_data.get(site_url, {}).get("sessionStorage", "")
    persistent_storage = storage_data.get(site_url, {}).get("localStorage", "")
    results_with_storage.append(row + (session_storage, persistent_storage))

cursor_output.executemany('''
    INSERT INTO stateful (
        site_url, 
        session_and_first_party_cookies, 
        persistent_and_first_party_cookies, 
        session_and_third_party_cookies, 
        persistent_and_third_party_cookies,
        session_storage,
        persistent_storage
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
''', results_with_storage)


############## Fingerprintig #############
cursor_input.execute('''
SELECT 
  sv.site_url, 

  CASE WHEN (
    sv.visit_id IN (
      SELECT 
        visit_id 
      FROM 
        javascript 
      WHERE 
        (
          script_url IN (
            SELECT 
              script_url 
            FROM 
              javascript 
            WHERE 
              symbol LIKE '%CanvasRenderingContext2D.fillStyle%' 
              OR symbol LIKE '%CanvasRenderingContext2D.strokeStyle%'
          ) 
          AND script_url IN (
            SELECT 
              script_url 
            FROM 
              javascript 
            WHERE 
              symbol LIKE '%CanvasRenderingContext2D.fillText%' 
              OR symbol LIKE '%CanvasRenderingContext2D.strokeText%'
          ) 
          AND script_url IN (
            SELECT 
              script_url 
            FROM 
              javascript 
            WHERE 
              symbol LIKE '%CanvasRenderingContext2D.getImageData%' 
              OR symbol LIKE '%HTMLCanvasElement.toDataURL%'
          ) 
          AND script_url NOT IN (
            SELECT 
              script_url 
            FROM 
              javascript 
            WHERE 
              symbol LIKE '%CanvasRenderingContext2D.save%' 
              OR symbol LIKE '%CanvasRenderingContext2D.restore%' 
              OR symbol LIKE '%CanvasRenderingContext2D.addEventListener%'
          )
        ) 
      GROUP BY 
        script_url
    )
  ) THEN 1 ELSE 0 END AS canvas_fingerprinting, 

  CASE WHEN (
    sv.visit_id IN (
      SELECT 
        visit_id 
      FROM 
        javascript 
      WHERE 
        (
          script_url IN (
            SELECT 
              script_url 
            FROM 
              javascript 
            WHERE 
              (
                symbol LIKE '%WebGLRenderingContext.getExtension%' 
                AND arguments = '["WEBGL_debug_renderer_info"]'
              ) 
              OR (
                symbol LIKE '%WebGL2RenderingContext.getExtension%' 
                AND arguments = '["WEBGL_debug_renderer_info"]'
              ) 
              OR (
                symbol LIKE '%WebGLRenderingContext.getSupportedExtensions%'
              ) 
              OR (
                symbol LIKE '%WebGL2RenderingContext.getSupportedExtensions%'
              )
          ) 
          OR (
            script_url IN (
              SELECT 
                script_url 
              FROM 
                javascript 
              WHERE 
                symbol LIKE '%WebGLRenderingContext.drawElements%' 
                OR symbol LIKE '%WebGL2RenderingContext.drawElements%' 
                OR symbol LIKE '%WebGLRenderingContext.drawArrays%' 
                OR symbol LIKE '%WebGL2RenderingContext.drawArrays%'
            ) 
            AND script_url IN (
              SELECT 
                script_url 
              FROM 
                javascript 
              WHERE 
                symbol LIKE '%WebGLRenderingContext.readPixels%' 
                OR symbol LIKE '%WebGL2RenderingContext.readPixels%' 
                OR symbol LIKE '%HTMLCanvasElement.toDataURL%'
            )
          )
        ) 
      GROUP BY 
        script_url
    )
  ) THEN 1 ELSE 0 END AS webgl_fingerprinting,

  CASE WHEN (
    sv.visit_id IN (
      SELECT 
        visit_id 
      FROM 
        javascript 
      WHERE 
        (
          script_url IN (
            SELECT 
              script_url 
            FROM 
              javascript 
            WHERE 
              symbol LIKE '%RTCPeerConnection.createDataChannel%' 
              OR symbol LIKE '%RTCPeerConnection.createOffer%'
          ) 
          AND script_url IN (
            SELECT 
              script_url 
            FROM 
              javascript 
            WHERE 
              symbol LIKE '%RTCPeerConnection.onicecandidate%' 
              OR symbol LIKE '%RTCPeerConnection.localDescription%'
          )
        ) 
      GROUP BY 
        script_url
    )
  ) THEN 1 ELSE 0 END AS webrtc_fingerprinting, 

  CASE WHEN (
    sv.visit_id IN (
      SELECT 
        visit_id 
      FROM 
        javascript 
      WHERE 
        (
          script_url IN (
            SELECT 
              script_url 
            FROM 
              javascript 
            WHERE 
              symbol LIKE '%AudioContext.createOscillator%' 
              OR symbol LIKE '%AudioContext.createDynamicsCompressor%' 
              OR symbol LIKE '%AudioContext.destination%' 
              OR symbol LIKE '%AudioContext.startRendering%' 
              OR symbol LIKE '%AudioContext.oncomplete%'
          )
        )
    )
  ) THEN 1 ELSE 0 END AS audio_fingerprinting

FROM 
  site_visits sv 
  LEFT JOIN javascript jc ON sv.visit_id = jc.visit_id 
GROUP BY 
  sv.site_url;
''')
fingerprinting_results = cursor_input.fetchall()

filtered_fingerprinting = []
for row in fingerprinting_results:
    site_url = row[0]
    if site_url in valid_site_urls:
        filtered_fingerprinting.append(row)

cursor_output.executemany('''
    INSERT INTO stateless (
        site_url, 
        canvas_fingerprinting, 
        webgl_fingerprinting,
        webrtc_fingerprinting, 
        audio_fingerprinting 
    ) VALUES (?, ?, ?, ?, ?)
''', filtered_fingerprinting)

############## Trackers #############
cursor_input.execute('''
SELECT 
    sv.site_url,
    MAX(CASE WHEN hr.url LIKE '%2mdn.net%' OR 
                  hr.url LIKE '%adservice.google.com%' OR 
                  hr.url LIKE '%doubleclick.net%' OR 
                  hr.url LIKE '%invitemedia.com%' 
             THEN 1 ELSE 0 END) AS doubleclick,
    MAX(CASE WHEN hr.url LIKE '%google-analytics.com%' OR 
                  hr.url LIKE '%googleanalytics.com%' 
             THEN 1 ELSE 0 END) AS google_analytics,
    MAX(CASE WHEN hr.url LIKE '%googlesyndication.com%' 
             THEN 1 ELSE 0 END) AS google_syndication,
    MAX(CASE WHEN hr.url LIKE '%facebook.com%' OR 
                  hr.url LIKE '%facebook.net%' 
             THEN 1 ELSE 0 END) AS facebook,
    MAX(CASE WHEN hr.url LIKE '%t.co%' OR 
                  hr.url LIKE '%twimg.com%' OR 
                  hr.url LIKE '%twitter.com%' OR 
                  hr.url LIKE '%x.com%' OR 
                  hr.url LIKE '%ads-twitter.com%' 
             THEN 1 ELSE 0 END) AS twitter,
    MAX(CASE WHEN hr.url LIKE '%criteo.com%' OR 
                  hr.url LIKE '%criteo.net%' 
             THEN 1 ELSE 0 END) AS criteo
FROM 
    site_visits sv
LEFT JOIN 
    http_requests hr ON sv.visit_id = hr.visit_id
GROUP BY 
    sv.site_url;
''')
tracker_results = cursor_input.fetchall()

filtered_trackers = []
for row in tracker_results:
    site_url = row[0]
    if site_url in valid_site_urls:
        filtered_trackers.append(row)

cursor_output.executemany('''
    INSERT INTO trackers (
        site_url, 
        doubleclick, 
        google_analytics, 
        google_syndication, 
        facebook, 
        twitter, 
        criteo
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
''', filtered_trackers)

conn_output.commit()
conn_input.close()
conn_output.close()
