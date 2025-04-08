import sqlite3 
import json 
import os
import base64

RED = "\033[31m"
RESET = "\033[0m"

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
    print(f"-- Coeur du message : {content_messages} \n")


    # Lire messaging.db/sqlite_sequence
    cursor.execute("SELECT name, seq FROM sqlite_sequence")
    content_sqlite_sequence= cursor.fetchall() # Stocker les infos pertinantes de la base de données 
    # print(f"composants de sqlite_sequence : {content_sqlite_sequence}")

# Fermer la base de données 
    conn.close()
    return content_contact, content_messages, content_sqlite_sequence

# Extraire les fichiers JSON 
def connexion_JSON():
    # Ouvrir les fichiers json  
    contenu_Json = []
    for fichiers in os.listdir("data/out"):
        path = "data/out/" + fichiers
        with open(path, "r") as f:
            contenu_Json.append(json.load(f))

    print(f"-- FICHIERS JSON = {contenu_Json} \n")
    return contenu_Json



# Comparaison de la structure des JSON avec la consigne : Format
def format_Json_Objet(contenu_Json, consigne_Json_keys):
    erreur=0
    for fichier in contenu_Json:
        keys = fichier.keys()
        i=0
        print(f"-- Toutes les clés du dico : {keys}")
        if len(keys) != len(consigne_Json_keys): # Objet en plus ou en moins
            print("{RED} -!- ERREUR : ATTENTION TAILLE DU JSON EST DIFFERENTE{RESET}")
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

# Comparaison de la structure des JSON avec la consigne : Types
def format_Json_Type(contenu_Json, consigne_Json_values):
    erreur=0
    for dico in contenu_Json:
        typeValeurs = dico.values()
        i=0
        # if len(keys) != len(consigne_Json_keys):
        #     erreur+=1
        for typeValeur in typeValeurs:
            if type(typeValeur) == type(consigne_Json_values[i]):
                # print(f"-- KEY OK : {valeur}")
                i+=1
            else: 
                print(f"{RED}-- ERREUR KEY KO : {typeValeur} différent de {type(consigne_Json_values[i])}{RESET}")
                i+=1
                erreur+=1
            continue
    return erreur


# Trier les json dans l'ordre des timestamp
def tri_et_comparaison_id_Json(contenu_Json):
    # listeJsonSorted = sorted(contenu_Json, key="id")
    # Tri de la liste de Json en fonction de leur Timestamp
    listeJsonSorted = sorted(contenu_Json, key=lambda p: p["timestamp"])
    print(listeJsonSorted)

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


# Comparaison des TimeStamp entre Json et Messaging
def timestamp_comp (content_messages, contenuJson):
    for tuple in content_messages:
        if all(tuple[1] != dico["timestamp"] for dico in contenuJson):
            print(f"{RED}-- TimeStamp KO {tuple[1]} non retrouvé dans les fichiers Json{RESET}")
    # for dico in contenuJson:
    #     for tuple in content_messages:
    #         if tuple[1] not in dico["timestamp"]:
    #             print(f"-- TimeStamp KO {tuple[1]} non retrouvé dans les fichiers Json")
    for i in range(0, len(content_messages)):
            for j in range(0, len(contenuJson)):
                if content_messages[i][1] == contenuJson[j]["timestamp"]:
                    print(f"-- TimeStamp OK correspondant Json ={contenuJson[j]["timestamp"]} : Message ID du Json id:{contenuJson[j]["id"]} : {contenuJson[j]["content"]}")
            
    return

# Comparaison du CONTENU entre Json et Messaging
def content_comp(content_messages, content_Json):
    # Pour timestamp égal
    for i in range(0, len(content_messages)):
        for j in range(0, len(content_Json)):
            if content_messages[i][1] == content_Json[j]["timestamp"] and content_messages[i][3] == content_Json[j]["content"]:
                print(f"-- Le contenu de json timestamp : {content_Json[j]["timestamp"]} correspond au contenu de messaging du meme timestamp : {content_Json[j]["content"]}")
            elif content_messages[i][1] == content_Json[j]["timestamp"]:
                print(f"{RED}-- ERREUR : Json content :<{content_Json[j]["content"]}> Messaging content :<{content_messages[i][3]}> pour le TimeStamp : {content_Json[i]["timestamp"]}{RESET}")
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
    for j in range(0, len(json)):
        all_json_directions.append(list(json[j].values()))
    # print(f"-- all directions = {all_json_directions}")
    
    for i in range(0, len(messages)):
        for j in range(0, len(json)):
            if messages[i][1] == json[j]["timestamp"] and messages[i][2] == all_json_directions[j][2]:
                print(f"-- La direction de Json timestamp : {json[j]["timestamp"]} correspond à la direction de messaging du meme timestamp : {all_json_directions[j][2]}")
            elif messages[i][1] == json[j]["timestamp"]:
                print(f"{RED}-- ERREUR : Json direction :<{all_json_directions[j][2]}> Messaging direction :<{messages[i][2]}> pour le TimeStamp : {json[i]["timestamp"]}{RESET}")
    return 

# Comparaison du CONTACT id du message entre Json et Messaging
def contact_comp(messages, json, consigne_contact_id):
    # Encodage contact id de messaging
    for message in messages:
        prev = message[-1]
        message[-1] = consigne_contact_id[str(message[-1])]
        # print(f"-- Contact id message avant = {prev} après = {message[-1]} du message id: {message[0]}")
    for i in range(0, len(messages)):
        for j in range(0, len(json)):
            if messages[i][1] == json[j]["timestamp"] and messages[i][-1] == json[j]["contact"]:
                print(f"-- Le contact id de Json timestamp : {json[j]["timestamp"]} correspond au contact de messaging du meme timestamp : {json[j]["contact"]}")
            elif messages[i][1] == json[j]["timestamp"]:
                print(f"{RED}-- ERREUR : Json Contact id :<{json[j]["contact"]}> Messaging Contact id :<{messages[i][2]}> pour le TimeStamp : {json[i]["timestamp"]}{RESET}")



    return


def main():

    content_contact, content_messages, content_sqlite_sequence = connexion_messaging()
    contenu_Json = connexion_JSON()
    contenu_JsonTri = tri_et_comparaison_id_Json(contenu_Json) # Tri par timestamp


    default_format = format_Json_Objet(contenu_Json,consigne_Json_keys)
    print(f" -!- Il y a {default_format} liée(s) au nom des objets dans les fichiers JSON ")
    
    default_type = format_Json_Type(contenu_Json,consigne_Json_values)
    print(f" -!- Il y a {default_type} liée(s) au type des objets dans les fichiers JSON ")

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
    contact_comp(content_message_encode, contenu_Json, consigne_contact_id)



if __name__ == "__main__":
    consigne_Json_keys = ['id', 'timestamp', 'direction', 'content', 'contact']
    # consigne_Json_keys = ['id', 'timestamp', 'direction', 'content', 'contact','longueur','elkjf']
    consigne_Json_values = [1, 1, "", "", ""]
    consigne_contact_id = {'1' : 'Tom', '2' : 'Zak', '3' : 'My Bank', '4' : 'Maman'}
    main()