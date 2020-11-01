import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timezone

client_id = 'c46f250061634356854fa16bc47802df'
client_secret = 'd48fde2af9724b748de5d36589b67e47'
urlToken = 'https://accounts.spotify.com/api/token'
body = {'grant_type': 'client_credentials', 'client_id': '0d41e4cb15704ab1ba5fab3d1db5c93a','client_secret': 'e43fe3a2d8d140f694e53e5f21ad8a83'}
BASE_URL = 'https://api.spotify.com/v1/'
response = requests.post(urlToken, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
                                        })

if response.status_code == 200:
    response_json = response.json()
    access_token = response_json["access_token"]
    headers = {
        "Authorization": "Bearer {token}".format(token=access_token)
    }
    print(headers)
    id_ledZeppelin = '36QJpDe2go2KgaRleHCDTp'
    id_BlackSabbath = '5M52tdBnJaKSvOpJGz8mfZ'
    id_Metallica = '2ye2Wgw4gimLv2eAKyk1NB'
    id_GrupoNiche = '1zng9JZpblpk48IPceRWs8'
    id_BodDylan = '74ASZWbe4lXaubB36ztrGX'
    id_MichaelJackson = '3fMbdgg4jU18AjLCKBhRSm'

    nombres_artistas = []
    popularidad_artista = []
    tipo_artista = []
    uri_artista =[]
    followers = []
    fecha_carga = []
    origen = []

    nombres_tracks = []
    tipo_track = []
    popularidad_track = []
    artistas_track = []
    album_track = []
    track_number = []
    id_track = []
    uri_track = []
    fecha_lanzamiento_track = []
    generos_tracks = []
    fecha_carga_track = []
    origen_track = []

    artistas_id = [id_GrupoNiche, id_ledZeppelin, id_BlackSabbath, id_MichaelJackson, id_BodDylan, id_Metallica]
    for artista_id in artistas_id:
        r = requests.get(BASE_URL + 'artists/' + artista_id + '/top-tracks?market=ES',
                         headers=headers,
                         params={'include_groups': 'album', 'limit': 50})
        data = requests.get(BASE_URL + 'artists/' + artista_id,
                            headers=headers,
                            )

        datos_artista = data.json()
        datos_track = r.json()["tracks"]

        nombres_artistas.append(datos_artista["name"])
        popularidad_artista.append(datos_artista['popularity'])
        tipo_artista.append(datos_artista['type'])
        uri_artista.append(datos_artista['uri'])
        followers.append(datos_artista['followers']['total'])
        now = datetime.now()
        fecha_carga.append(now.replace(tzinfo=timezone.utc).timestamp())
        origen.append(datos_artista['href'])

        for track in datos_track:
            nombres_tracks.append(track['name'])
            tipo_track.append(track['type'])
            popularidad_track.append(track['popularity'])
            artistas_track.append(track['artists'][0]["name"])
            album_track.append(track['album']['name'])
            track_number.append(track['disc_number'])
            id_track.append(track['id'])
            uri_track.append(track['uri'])
            fecha_lanzamiento_track.append(track['album']['release_date'])
            #generos_tracks.append(track['genre'])
            now = datetime.now()
            fecha_carga_track.append(now.replace(tzinfo=timezone.utc).timestamp())
            origen_track.append(track['href'])

tabla_artistas = {'nombre': nombres_artistas,'Tipo':tipo_artista,'Uri':uri_artista,'popularidad':popularidad_artista,
                      'seguidores':followers,'FechaCarga':fecha_carga,'Origen':origen}
tabla_tracks = {'ID':id_track,'nombre': nombres_tracks, 'Tipo': tipo_track, 'Uri': uri_track, 'popularidad': popularidad_track,
                    'artista':artistas_track,'FechaDeLanzamiento':fecha_lanzamiento_track,
                    'FechaCarga':fecha_carga_track, 'Origen':origen_track, 'NumeroTrack':track_number}

artistas = pd.DataFrame(data=tabla_artistas)
tracks = pd.DataFrame(data=tabla_tracks)
engine = create_engine('postgresql://postgres:3098@127.0.0.1:5432/Taller2')
artistas.to_sql('Artistas',con=engine,index=False)
tracks.to_sql('Tracks',con=engine,index=False)
