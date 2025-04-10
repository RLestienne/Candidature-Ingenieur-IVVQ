import sqlite3 
import json 
import os
import base64

RED = "\033[31m"
RESET = "\033[0m"

## ---------------------------CONNEXIONS------------------------------------------------------------
# Extraire les tableaux de base de données
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
    content_messages = [list(message) for message in content_messages] # Faire une liste de liste pour encode en base 64 les content 
    # print(f"-- Coeur du message : {content_messages} \n")


    # Lire messaging.db/sqlite_sequence
    cursor.execute("SELECT name, seq FROM sqlite_sequence")
    content_sqlite_sequence= cursor.fetchall() # Stocker les infos pertinantes de la base de données 
    # print(f"composants de sqlite_sequence : {content_sqlite_sequence}")

# Fermer la base de données 
    conn.close()
    return content_contact, content_messages, content_sqlite_sequence

# Extraire les fichiers JSON 
def connexion_JSON(out):
    # Ouvrir les fichiers json  
    contenu_Json = []
    for fichiers in os.listdir(out):
        path = out + "/" + fichiers
        with open(path, "r") as f:
            contenu_Json.append(json.load(f))

    # print(f"-- FICHIERS JSON = {contenu_Json} \n")
    return contenu_Json

## ---------------------------COMPARAISONS / FORMAT CONSIGNE------------------------------------------------------------


# Comparaison de la structure des JSON avec la consigne : NOM
def format_Json_Objet(contenu_Json, consigne_Json_keys):
    erreur=0
    keys = []
    erreur_nom_plusoumoins = ""
    erreur_nom_manquant = ""
    for fichier in contenu_Json:
        keys.append(list(fichier.keys()))
        # print(f"-- Toutes les clés du dico : {keys}")
    if len(keys) != len(consigne_Json_keys): # Objet en plus ou en moins
            print(f"{RED}-- ERREUR : Il y a plus ou moins d'objet(s) dans le timestamp : {fichier["timestamp"]}{RESET}")
            erreur+=1
            erreur_nom_plusoumoins += str(fichier["timestamp"])
    for key in consigne_Json_keys: # Pour chacune des clés de la consigne voir si présent dans chacun des fichier json 
        for jsonkey in keys:
            if key in jsonkey:
                erreur = erreur # print(f"-- KEY OK : {key}")
            else: 
                erreur = erreur + 1
                erreur_nom_manquant += str(fichier["timestamp"])
                # print(f"-- ERREUR KEY KO : {key}")
                
        continue
    return erreur, erreur_nom_plusoumoins, erreur_nom_manquant

# Comparaison de la structure des JSON avec la consigne : TYPE
def format_Json_Type(contenu_Json, consigne_Json_values):
    erreur=0
    for dico in contenu_Json:
        typeValeurs = dico.values()
        i=0
        # if len(keys) != len(consigne_Json_keys):
        #     erreur+=1
        for typeValeur in typeValeurs:
            if type(typeValeur) == type(consigne_Json_values[i]):
                # print(f"-- KEY OK : {typeValeur}")
                i+=1
            else: 
                print(f"{RED}-- ERREUR KEY KO : {typeValeur} différent de {type(consigne_Json_values[i])}{RESET}")
                i+=1
                erreur+=1
            continue
    return erreur


## ---------------------------COMPARAISONS / JSON & MESSAGING------------------------------------------------------------


# Implémentation de l'item flag au dictionnaire des Json 
def flag_implementation(dico_json):
    for i in range(0, len(dico_json)):
        dico_json[i]["flag"] = 0
        dico_json[i]["erreur"] = ""
        # print(f"{dico_json[i]}")

    # print(f"{dico_json}")
    return dico_json


# Trier les json dans l'ordre des TIMESTAMP
def tri_et_comparaison_id_Json(contenu_Json):
    # listeJsonSorted = sorted(contenu_Json, key="id")
    # Tri de la liste de Json en fonction de leur Timestamp
    listeJsonSorted = sorted(contenu_Json, key=lambda p: p["timestamp"])
    # print(listeJsonSorted)

    if listeJsonSorted[0]["id"] != 1: print(f"{RED}-!- Erreur : L'ID des messages ne commence pas par 1{RESET}") # Si il manque des ID alors lesquels 

    for i in range(1, len(listeJsonSorted)): 
        prev_id = listeJsonSorted[i-1]["id"] 
        id = listeJsonSorted[i]["id"]
        if id == prev_id:        # Si il ya une répétition d'id alors lesquels 
            print(f"{RED}-!- Erreur : L'ID {id} est identique à son prédécesseur{RESET}")
       
        elif id > prev_id + 1: # Si les id ne s'incrémente pas de 1 à chaque message alors erreur 
            print(f"{RED}-!- Erreur : L'ID {id} n'a pas {id-1} comme prédécesseur{RESET}")
       
        else: continue
    
    print(f"-- JSON Triés par id : {listeJsonSorted}")
    return listeJsonSorted

# Décodage de content des JSON
def decodage_base64(contenu_JsonTri):
    for i in range(0, len(contenu_JsonTri)):
        contenu_JsonTri[i]["content"] = base64.b64decode(contenu_JsonTri[i]["content"])
        texte = contenu_JsonTri[i]["content"].decode("utf-8")
        print(f"{contenu_JsonTri[i]["id"]} {texte}")
        # print(contenu_JsonTri)
    # return contenu_JsonTri

# Encodage des messages
def encodage_base64(content_messages):
    for message in content_messages:
        # print(f"-- avant {message[3]}")
        message[3] = base64.b64encode(message[3].encode('utf-8')).decode('utf-8')
        # print(f"-- après {message[3]}")

    return content_messages


# Comparaison des TIMESTAMP entre Json et Messaging
def timestamp_comp (content_messages, contenuJson):

    for message in content_messages:  
        if all(message[1] != dico["timestamp"] for dico in contenuJson): # Timestamp de messages tous présents dans les JSON ?
            print(f"{RED}-- TimeStamp de MESSAGING KO {message[1]} non retrouvé dans les fichiers Json{RESET}")
    for json in contenuJson:  
        if all(json["timestamp"] != timestamp_message[1] for timestamp_message in content_messages): # Timestamp de Json tous présents dans les messages ?
            print(f"{RED}-- TimeStamp de JSON KO {json["timestamp"]} non retrouvé dans les Timastamp de messaging{RESET}")
            json["flag"] = json["flag"] + 1
            json["erreur"] +=" TimeStamp non présent dans messaging /"
    for i in range(0, len(content_messages)): # Quel Timestamp sont à la fois dans Json et Messaging
            for j in range(0, len(contenuJson)): 
                if content_messages[i][1] == contenuJson[j]["timestamp"]:
                    print(f"-- TimeStamp OK correspondant Json ={contenuJson[j]["timestamp"]} : Message ID du Json id:{contenuJson[j]["id"]} : {contenuJson[j]["content"]}")

    for i in range(0, len(contenuJson)): # Incrémentation correcte de l'ID 
        prev = contenuJson[i-1]["timestamp"]
        cour = contenuJson[i]["timestamp"]

        if cour == prev:
            print(f"{RED}-- ERREUR les Timestamp {prev} & {cour} des Json sont égaux{RESET}")
            contenuJson[i-1]["flag"] += 1 
            contenuJson[i-1]["erreur"] += " TimeStamp répétitif /"
            contenuJson[i]["flag"] += 1
            contenuJson[i]["erreur"] += " TimeStamp répétitif /"
        elif cour > prev:
            print(f"-- Les Timestamp {prev} & {cour} des Json sont dans l'ordre croissant")
        else:
            print(f"{RED}-- ERREUR les Timestamp {prev} & {cour} des Json sont pas dans l'ordre décroissant{RESET}")
            contenuJson[i-1]["flag"] += 1 
            contenuJson[i-1]["erreur"] += " TimeStamp désordonné /"
            contenuJson[i]["flag"] += 1
            contenuJson[i]["erreur"] += " TimeStamp désordonné /"

    return

# Comparaison du CONTENU entre Json et Messaging
def content_comp(content_messages, content_Json):
    # Pour timestamp égal
    for i in range(0, len(content_Json)):
        for j in range(0, len(content_messages)):
            if content_messages[j][1] == content_Json[i]["timestamp"] and content_messages[j][3] == content_Json[i]["content"]:
                print(f"-- Le contenu de json timestamp : {content_Json[i]["timestamp"]} correspond au contenu de messaging du meme timestamp : {content_Json[i]["content"]}")
            elif content_messages[j][1] == content_Json[i]["timestamp"]:
                print(f"{RED}-- ERREUR : Json content :<{content_Json[i]["content"]}> Messaging content :<{content_messages[j][3]}> pour le TimeStamp : {content_Json[i]["timestamp"]}{RESET}")
                content_Json[i]["flag"] +=1
                content_Json[i]["erreur"] += " Contenu du message /"

    return 


# Comparaison de la DIRECTION du message entre Json et Messaging
def direction_comp(messages, json):
    # # Pour timestamp égal
    # for i in range(0, len(messages)):
    #     for j in range(0, len(json)):
    #         try:
    #             direction = json[j]["direction"]
    #         except KeyError:
    #             print(f"{RED}-- ERREUR : Timestamp : <{json[j]["timestamp"]}> La clé 'direction' est absente ou mal orthographiée{RESET}")
    #             continue
    #         if messages[i][1] == json[j]["timestamp"] and messages[i][2] == json[j]["direction"]:
    #             print(f"-- La direction de Json timestamp : {json[j]["timestamp"]} correspond à la direction de messaging du meme timestamp : {json[j]['direction']}")
    #         elif messages[i][1] == json[j]["timestamp"]:
    #             print(f"{RED}-- ERREUR : Json direction :<{json[j]['direction']}> Messaging direction :<{messages[i][2]}> pour le TimeStamp : {json[i]["timestamp"]}{RESET}")
    all_json_directions = []
    for j in range(0, len(json)): # Pour éviter les erreurs de format
        all_json_directions.append(list(json[j].values()))
    # print(f"-- all directions = {all_json_directions}")
    
    for i in range(0, len(messages)):
        for j in range(0, len(json)):
            if messages[i][1] == json[j]["timestamp"] and messages[i][2] == all_json_directions[j][2]:
                print(f"-- La direction de Json timestamp : {json[j]["timestamp"]} correspond à la direction de messaging du meme timestamp : {all_json_directions[j][2]}")
            elif messages[i][1] == json[j]["timestamp"]:
                print(f"{RED}-- ERREUR : Json direction :<{all_json_directions[j][2]}> Messaging direction :<{messages[i][2]}> pour le TimeStamp : {json[j]["timestamp"]}{RESET}")
                json[j]["flag"] +=1
                json[j]["erreur"] += " Direction du message /"

    return 

# Comparaison du CONTACT id du message entre Json et Messaging
def contact_comp(messages, json, consigne_contact_id):
    # Encodage contact id de messaging
    for message in messages:
        prev = message[-1]
        for j in range(0, len(consigne_contact_id)):
            if message[-1] == consigne_contact_id[j][0]:
                message[-1] = consigne_contact_id[j][1]
        print(f"-- Contact id message avant = {prev} après = {message[-1]} du message id: {message[0]}")
    for i in range(0, len(json)):
        for j in range(0, len(messages)):
            if messages[j][1] == json[i]["timestamp"] and messages[j][-1] == json[i]["contact"]:
                print(f"-- Le contact id de Json timestamp : {json[i]["timestamp"]} correspond au contact de messaging du meme timestamp : {json[i]["contact"]}")
            elif messages[j][1] == json[i]["timestamp"]:
                print(f"{RED}-- ERREUR : Json Contact id :<{json[i]["contact"]}> Messaging Contact id :<{messages[j][2]}> pour le TimeStamp : {json[i]["timestamp"]}{RESET}")
                json[i]["flag"] +=1
                json[i]["erreur"] += " Contact /"

    return

# Resume Default
def resume_default(contenu_json):
    for i in range(0, len(contenu_json)):
        print(f"-- Le fichier Json [timestamp={contenu_json[i]["timestamp"]}] présente {RED}{contenu_json[i]["flag"]} erreur(s) :{contenu_json[i]["erreur"]}{RESET}")
    return


## ---------------------------MAIN------------------------------------------------------------


def main():

    content_contact, content_messages, content_sqlite_sequence = connexion_messaging()
    contenu_Json_raw = connexion_JSON(out)
    # contenu_JsonTri = tri_et_comparaison_id_Json(contenu_Json_raw) # Tri par timestamp


    result_format_Json_Nom, erreur_nom_timestamp_tropoumoins, erreur_nom_timestamp_manquant = format_Json_Objet(contenu_Json_raw,consigne_Json_keys)
    
    result_format_Json_Type = format_Json_Type(contenu_Json_raw,consigne_Json_values)

# Ajout de l'item Flag au dictionnaire des Json
    contenu_Json = flag_implementation(contenu_Json_raw)

# Décodage basse 64
    # contenu_Json_decodeB64 = decodage_base64(contenu_JsonTri)
    # print(contenu_Json_decodeB64)


# Encodage base 64 
    content_message_encode = encodage_base64(content_messages)
    # print(f"-- Content messages encodé : {content_message_encode}")

# Comparaison TIMESTAMP
    timestamp_comp(content_message_encode, contenu_Json)

# Comparaison CONTENU 
    content_comp(content_message_encode, contenu_Json)

# Comparaison DIRECTION 
    direction_comp(content_message_encode, contenu_Json)

# Comparaisond du CONTACT id
    contact_comp(content_message_encode, contenu_Json, content_contact)


    print("############################################\n###################RESUME###################\n############################################")
# Resume default
    if result_format_Json_Nom !=0:
        print(f"-- Les fichiers Json comptent au total {RED}{result_format_Json_Nom} erreur(s){RESET} de NOM\n  \
              Ce(s) json {erreur_nom_timestamp_tropoumoins} ont un ou plusieurs objet(s) en trop ou en moins\n  \
              Ce(s) json {erreur_nom_timestamp_manquant} ont un ou plusieurs objet(s) manquant")
    else: print("-- Il n'y a pas d'erreur de NOM dans les fichiers Json")
    if result_format_Json_Type !=0:
        print(f"-- Les fichiers Json comptent au total {RED}{result_format_Json_Type} erreur(s){RESET} de TYPE")
    else: print("-- Il n'y a pas d'erreur de TYPE dans les fichiers Json")
    
    resume_default(contenu_Json)



if __name__ == "__main__":
    consigne_Json_keys = ['id', 'timestamp', 'direction', 'content', 'contact']
    # consigne_Json_keys = ['id', 'timestamp', 'direction', 'content', 'contact','longueur','elkjf'] #test
    consigne_Json_values = [1, 1, "", "", ""]
    # consigne_contact_id = {'1' : 'Tom', '2' : 'Zak', '3' : 'My Bank', '4' : 'Maman'}
    out = "data/out"
    # out = "data/outTest"
    main()