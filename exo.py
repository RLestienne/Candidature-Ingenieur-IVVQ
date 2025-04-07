import sqlite3 
import json 
import os
import base64

def connexion_messaging():

# Ouvrir la base de données avec sqlite3
    conn = sqlite3.connect('data/in/messaging.db')
    cursor = conn.cursor()

    # Lire messaging.db/contact
    cursor.execute("SELECT id, name FROM contact")
    content_contact= cursor.fetchall() # Stocker les infos pertinantes de la base de données 
    # print(f"composants de contact : {content_contact}")


    # Lire messaging.db/messages
    cursor.execute("SELECT id, timestamp, direction, content, contact_id FROM messages")
    content_messages= cursor.fetchall() # Stocker les infos pertinantes de la base de données 
    print(f"-- Coeur du message : {content_messages} \n")


    # Lire messaging.db/sqlite_sequence
    cursor.execute("SELECT name, seq FROM sqlite_sequence")
    content_sqlite_sequence= cursor.fetchall() # Stocker les infos pertinantes de la base de données 
    # print(f"composants de sqlite_sequence : {content_sqlite_sequence}")

# Fermer la base de données 
    conn.close()
    return content_contact, content_messages, content_sqlite_sequence

def connexion_JSON():
    # Ouvrir les fichiers json  
    contenu_Json = []
    for fichiers in os.listdir("data/out"):
        path = "data/out/" + fichiers
        with open(path, "r") as f:
            contenu_Json.append(json.load(f))

    print(f"-- FICHIERS JSON = {contenu_Json} \n")
    return contenu_Json



# Comparaison au format de la consigne
def format_Json_Objet(contenu_Json, consigne_Json_keys):
    erreur=0
    for fichier in contenu_Json:
        keys = fichier.keys()
        i=0
        # print(f"-- Toutes les clés du dico : {keys}")
        if len(keys) != len(consigne_Json_keys):
            print("ATTENTION TAILLE DU JSON EST DIFFERENTE")
            erreur+=1
        for key in keys:
            if key == consigne_Json_keys[i]:
                # print(f"-- KEY OK : {key}")
                i+=1
            else: 
                # print(f"-- ERREUR KEY KO : {key}")
                i+=1
                erreur+=1
            continue
    return erreur

def format_Json_Type(contenu_Json, consigne_Json_values):
    erreur=0
    for dico in contenu_Json:
        typeValeurs = dico.values()
        i=0
        # print(f"-- Toutes les clés du dico : {keys}")
        # if len(keys) != len(consigne_Json_keys):
        #     erreur+=1
        for typeValeur in typeValeurs:
            if type(typeValeur) == type(consigne_Json_values[i]):
                # print(f"-- KEY OK : {valeur}")
                i+=1
            else: 
                print(f"-- ERREUR KEY KO : {typeValeur} différent de {type(consigne_Json_values[i])}")
                i+=1
                erreur+=1
            continue
    return erreur


# Le nombre de message traduit 

# Si chacun des message est strictement trasncrit


# Trier les json dans l'ordre d'id
def tri_et_comparaison_id_Json(contenu_Json):
    # listeJsonSorted = sorted(contenu_Json, key="id")

    listeJsonSorted = sorted(contenu_Json, key=lambda p: p["id"])
    print(listeJsonSorted)

    # Si les id ne s'incrémente pas de 1 à chaque message alors erreur 
    # Si il manque des ID alors lesuqels 
    # Si il ya une répétition d'id alors lesquels 
    if listeJsonSorted[0]["id"] != 1: print(f"-!- Erreur : L'ID des messages ne commence pas par 1")
    for i in range(1, len(listeJsonSorted)): 
        prev_id = listeJsonSorted[i-1]["id"] 
        id = listeJsonSorted[i]["id"]
        if id == prev_id:
            print(f"-!- Erreur : L'ID {id} est identique à son prédécesseur")
        elif id > prev_id + 1: 
            print(f"-!- Erreur : L'ID {id} n'a pas {id-1} comme prédécesseur")
        else: continue
    
    print(f"-- JSON Triés par id : {listeJsonSorted}")
    return listeJsonSorted

def decodage_base64(contenu_JsonTri):
    for i in range(0, len(contenu_JsonTri)):
        contenu_JsonTri[i]["content"] = base64.b64decode(contenu_JsonTri[i]["content"])
        texte = contenu_JsonTri[i]["content"]
        print(f"{contenu_JsonTri[i]["id"]} {texte}")
        # print(contenu_JsonTri)
    return contenu_JsonTri



def main():

    connexion_messaging()
    contenu_Json = connexion_JSON()
    contenu_JsonTri = tri_et_comparaison_id_Json(contenu_Json)


    default_format = format_Json_Objet(contenu_Json,consigne_Json_keys)
    print(f" -!- Il y a {default_format} liée(s) au nom des objets dans les fichiers JSON ")
    
    default_type = format_Json_Type(contenu_Json,consigne_Json_values)
    print(f" -!- Il y a {default_type} liée(s) au type des objets dans les fichiers JSON ")

# Décodage basse 64
    contenu_Json_decodeB64 = decodage_base64(contenu_JsonTri)
    # print(contenu_Json_decodeB64)



if __name__ == "__main__":
    consigne_Json_keys = ['id', 'timestamp', 'direction', 'content', 'contact']
    # consigne_Json_keys = ['id', 'timestamp', 'direction', 'content', 'contact','longueur','elkjf']
    consigne_Json_values= [1, 1, "", "", ""]
    main()