import socket
import json
import struct
import threading
import os
import time
import sys

# Obtenir le nom de la machine
nom_machine = socket.gethostname()
PORT = 4444
PORT2 = 4445
print(f"'{nom_machine}' : Bonjour, je suis la machine ")

messagePostShuffle=[] #variable pour stocker les messages après le suffle
messagePostShuffle2=[] #variable pour stocker les messages après le suffle2

# Créer un socket TCP/IP
serveur_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Lier le socket à l'adresse et au port avec un maximum de 5 tentatives
for tentative in range(5):
    try:
        serveur_socket.bind(('0.0.0.0', PORT))
        print(f"'{nom_machine}' : Le socket est lié au port {PORT} après {tentative + 1} tentative(s).")
        break
    except OSError:
        if tentative < 4:
            # Si le port est déjà utilisé, libérer le port en utilisant la commande kill
            print(f"'{nom_machine}' : Le port {PORT} est déjà utilisé. Tentative de libération du port ({tentative + 1}/5)...")
            # Afficher avec print le PID du processus qui utilise le port
            pid = os.popen(f'lsof -t -i:{PORT}').read().strip()
            print(f"'{nom_machine}' : PID du processus qui utilise le port {PORT} : {pid}")
            if pid:
                # Libérer le port et afficher le résultat de kill
                os.system(f'kill -9 {pid}')
                print(f"'{nom_machine}' : Tentative de tuer le processus {pid}.")
            else:
                print(f"'{nom_machine}' : Aucun processus n'utilise le port {PORT}.")
            time.sleep(5)
        else:
            raise Exception(f"'{nom_machine}' : Impossible de lier le socket au port {PORT} après 5 tentatives.")

# Créer un socket TCP/IP
serveur_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Lier le socket à l'adresse et au port avec un maximum de 5 tentatives
for tentative in range(5):
    try:
        serveur_socket2.bind(('0.0.0.0', PORT2))
        print(f"'{nom_machine}' : Le socket est lié au port {PORT2} après {tentative + 1} tentative(s).")
        break
    except OSError:
        if tentative < 4:
            # Si le port est déjà utilisé, libérer le port en utilisant la commande kill
            print(f"'{nom_machine}' : Le port {PORT2} est déjà utilisé. Tentative de libération du port ({tentative + 1}/5)...")
            # Afficher avec print le PID du processus qui utilise le port
            pid = os.popen(f'lsof -t -i:{PORT2}').read().strip()
            print(f"'{nom_machine}' : PID du processus qui utilise le port {PORT2} : {pid}")
            if pid:
                # Libérer le port et afficher le résultat de kill
                os.system(f'kill -9 {pid}')
                print(f"'{nom_machine}' : Tentative de tuer le processus {pid}.")
            else:
                print(f"'{nom_machine}' : Aucun processus n'utilise le port {PORT2}.")
            time.sleep(5)
        else:
            raise Exception(f"'{nom_machine}' : Impossible de lier le socket au port {PORT2} après 5 tentatives.")


# Écouter les connexions entrantes
serveur_socket.listen(5)
print(f"'{nom_machine}' : PHASE 1 Le serveur écoute sur le port {PORT}...")

serveur_socket2.listen(5)
print(f"'{nom_machine}' : PHASE 2 Le serveur écoute sur le port {PORT2}...")

connexions = {}
connexions_phase_2 = {}

def recevoir_exactement(client_socket, n):
    data = b''
    while len(data) < n:
        packet = client_socket.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def recevoir_message(client_socket):
    # Recevoir la taille du message
    taille_message_bytes = recevoir_exactement(client_socket, 4) # 4 octets pour la taille du message
    if taille_message_bytes is None:
        print("Connexion fermée lors de la réception de la taille du message.")
        return None
    taille_message = struct.unpack('!I', taille_message_bytes)[0]
    # Recevoir le message en utilisant la taille
    data = recevoir_exactement(client_socket, taille_message)
    if data is None:
        print("Connexion fermée lors de la réception du message.")
        return None
    return data.decode('utf-8')

def envoyer_message(client_socket, message):
    try:
        # Convertir le message en bytes
        message_bytes = message.encode('utf-8')
        # Obtenir la taille du message
        taille_message = len(message_bytes)
        # Convertir la taille en 4 octets
        taille_message_bytes = struct.pack('!I', taille_message)
        # Envoyer la taille du message suivie du message
        client_socket.sendall(taille_message_bytes + message_bytes)
    except BrokenPipeError:
        print("Erreur: Broken pipe. La connexion a été fermée par l'autre côté.")
    except Exception as e:
        print(f"Erreur lors de l'envoi du message: {e}")

def envoyer_message_liste(client_socket, message_liste):
    try:
        # Convertir le dictionnaire en chaîne JSON
        message_json = json.dumps(message_liste)
        # Envoyer la chaîne JSON
        envoyer_message(client_socket, message_json)
    except Exception as e:
        print(f"Erreur lors de l'envoi du message: {e}")

def gerer_connexion(client_socket, adresse_client):
    # Variables :
    nb_message=0 #pour compter le nombre de messages reçus
    global etat #pour gérer les étapes
    etat=1 #initialiser l'état à 1
    machines_reçues=[] # Créer une liste vide pour stocker le nom des machines reçues
    fichiersWET_reçues = [] # Créer une liste vide pour stocker les noms des fichiers WET reçus
    contenuWET = [] # Créer une liste vide pour stocker le contenu des fichiers WET, pas encore splité en mots
    motsWET = [] # Créer une liste vide pour stocker tous les mots des fichiers WET une fois splités
    motsWET_json = [] # Créer une liste vide pour stocker tous les mots des fichiers WET une fois splités au format json
    progressionShuffle = 0 # Initialiser la progression du shuffle à 0
    progressionReduce = 0 # Initialiser la progression du reduce à 0

    print(f"'{nom_machine}' : Connexion acceptée de {adresse_client}")
    connexions[adresse_client] = client_socket #stocker la connexion

    while etat!=10:
        if etat==1 and nb_message==0:
            #### MAP 
            message_reçu = recevoir_message(client_socket) # Recevoir la liste des machines
            print(f"'PHASE 1 {nom_machine}' : Message reçu: {message_reçu}")
            machines_reçues = json.loads(message_reçu)
            nb_message=1
            
        elif etat==1 and nb_message==1:
            message_reçu = recevoir_message(client_socket) # Recevoir les fichiers WET
            while message_reçu != "FIN PHASE 1" :
                fichiersWET_reçues = json.loads(message_reçu)
                print(f"'PHASE 1 {nom_machine}' : Message reçu: {message_reçu}")
                nb_message=2
                message_reçu = recevoir_message(client_socket)

        elif message_reçu == "FIN PHASE 1" :
            while message_reçu != "GO PHASE 2":
                print(f"'PHASE 1 {nom_machine}' : Message reçu: {message_reçu}")
                etat=2
                thread_accepter_phase2 = threading.Thread(target=accepter_connexion_phase2)
                thread_accepter_phase2.start()
                envoyer_message(client_socket, "OK FIN PHASE 1")
                # Créer les connexions à toutes les machines
                for machine in machines_reçues:
                    try:
                        # Créer un socket TCP/IP
                        client_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        # Se connecter à la machine
                        client_socket2.connect((machine, PORT2))
                        # Stocker la connexion
                        connexions_phase_2[machine] = client_socket2
                        #print(f"'PHASE 2 {nom_machine}' : Connexion établie avec {machine}")
                    except Exception as e:
                        print(f"Erreur lors de la connexion à {machine}: {e}")
                message_reçu = recevoir_message(client_socket)

        elif message_reçu == "GO PHASE 2":
            etat=3
            #### Ouvrir les fichiers WET extraire les mots et faire le shuffle 
            print(f"'PHASE 2 {nom_machine}' : Message reçu: {message_reçu}")
            for j, fichierWET in enumerate(fichiersWET_reçues):
                if fichierWET.endswith('.wet'): # quelques fichiers ne sont pas des fichiers WET on ne les prend pas en compte
                    with open('/cal/commoncrawl/' + fichierWET, 'r') as file:
                        contenuWET = file.read()
                    for i, machine in enumerate(machines_reçues):
                        motsWET.extend([mot for mot in contenuWET.split() if len(mot)%len(machines_reçues) == i]) # Splitter le contenu du fichier WET en mots et les ajouter à la liste des mots si la longueur du mot est divisible par le nombre de machines
                        motsWET_json = json.dumps(motsWET)
                        envoyer_message(connexions_phase_2[machine], motsWET_json) # Envoyer les mots i à la machine i 
                        motsWET = [] # Vider la liste des mots pour le prochain envoi
                        motsWET_json = [] # Vider la liste des mots en JSON pour le prochain envoi
                        progressionShuffle = progressionShuffle+1 # Incrémenter la progression
                        afficher_barre_progression(progressionShuffle, len(fichiersWET_reçues)*len(machines_reçues), "'PHASE 2' : SHUFFLE en cours ") # Afficher la progression

            while message_reçu !="GO PHASE 3":    
                envoyer_message(client_socket, "OK PHASE 2")
                print(f"'PHASE 2 {nom_machine}' : Message envoyé: OK PHASE 2")
                message_reçu = recevoir_message(client_socket)
            
        elif message_reçu == "GO PHASE 3": 
            etat=4
            #### REDUCE 
            print(f"'PHASE 3 {nom_machine}' : Message reçu: {message_reçu}")
            compteur_mots = {} # Créer un dictionnaire vide pour compter les mots
            for liste in messagePostShuffle:
                for mot in liste:
                    if mot in compteur_mots:
                        compteur_mots[mot] += 1
                    else:
                        compteur_mots[mot] = 1
                    progressionReduce = progressionReduce+1 # Incrémenter la progression
                    if progressionReduce % 1000000 == 0: # Afficher la progression tous les 1 000 000 mots pour ne pas surcharger la console
                        afficher_barre_progression(progressionReduce, len(liste), "'PHASE 3' : REDUCE en cours ") # Afficher la progression du reduce
                progressionReduce = 0
            afficher_barre_progression(100, 100, "'PHASE 3' : REDUCE en cours ")
            #messagePostShuffle=[] # Vider la liste des messages après le shuffle1, sera réutilisé pour le shuffle2
                
            envoyer_message(client_socket, "OK PHASE 3")
            print(f"'PHASE 3 {nom_machine}' : Message envoyé: OK PHASE 3")
            while message_reçu !="GO PHASE 4":
                message_reçu = recevoir_message(client_socket)
            
        elif message_reçu == "GO PHASE 4":
            etat=5
            print(f"'PHASE 4 {nom_machine}' : Message reçu: {message_reçu}")
            liste = [] # Créer une liste pour stocker les clés et les valeurs du dictionnaire car on ne peut pas envoyer un dictionnaire directement en TCP
            for key, value in compteur_mots.items(): #le .items() permet de récupérer la clé et la valeur du dictionnaire
                liste.append(key) # Ajouter la clé
                liste.append(value) #Ajouter la valeur à la liste
            #print(f"'PHASE 4 {nom_machine}' : Message envoyé post REDUCE: {liste}")
            envoyer_message_liste(client_socket, liste) 
            
            envoyer_message(client_socket, "OK PHASE 4")
            print(f"'PHASE 4 {nom_machine}' : Message envoyé: OK PHASE 4")
            while message_reçu !="GO PHASE 5":
                message_reçu = recevoir_message(client_socket)
        
        ### SECOND MAPREDUCE pour trier les mots par occurence
        ### MAP2
        elif message_reçu == "GO PHASE 5":  
            etat=6  
            print(f"'PHASE 5 {nom_machine}' : Message reçu: {message_reçu}")
            message_reçu = recevoir_message(client_socket)
            liste2 = json.loads(message_reçu) # Convertir le message en liste
            liste_tuples = convertir_en_tuples(liste2) # Convertir la liste en liste de tuples
            liste_triee=trier_par_occurrences(liste_tuples) # Trier la liste de tuples par occurrences
            print(f"'PHASE 5 {nom_machine}' : Element avec le plus d'occurences: {liste_triee[0]} ")
            envoyer_message(client_socket, json.dumps(liste_triee[0][1])) # Envoyer le nombre d'occurences du premier tuple c'est à dire celui qui a le plus d'occurances
            occuranceMax=recevoir_message(client_socket) # Recevoir le nombre d'occurences max atteint pour un mot, toutes machines confondues
            
            envoyer_message(client_socket, "OK PHASE 5")
            print(f"'PHASE 5 {nom_machine}' : Message envoyé: OK PHASE 5")
            while message_reçu !="GO PHASE 6":
                message_reçu = recevoir_message(client_socket)
        
        ### SHUFFLE2
        elif message_reçu == "GO PHASE 6": 
            etat=7   
            print(f"'PHASE 6 {nom_machine}' : Message reçu: {message_reçu}")
            # Répartir les mots par occurrences sur différentes machines
            mots_par_machine = repartir_mots_par_occurrences(liste_triee, int(occuranceMax), connexions_phase_2)
            progressionShuffle2 = 0 # Initialiser la progression du shuffle à 0
            # Envoyer les mots à chaque machine
            for machine, mots in mots_par_machine.items():
                mots_json = json.dumps(mots)
                #envoyer_message(connexions[machine], mots_json)
                if mots:
                    envoyer_message(connexions_phase_2[machine], mots_json)
                progressionShuffle2 = progressionShuffle2+1 # Incrémenter la progression
                afficher_barre_progression(progressionShuffle2, len(mots_par_machine), "'PHASE 6' : SHUFFLE2 en cours ") # Afficher la progression
            
            envoyer_message(client_socket, "OK PHASE 6")
            print(f"'PHASE 6 {nom_machine}' : Message envoyé: OK PHASE 6")
            while message_reçu !="GO PHASE 7":
                message_reçu = recevoir_message(client_socket)


        ### REDUCE2
        elif message_reçu == "GO PHASE 7":  
            etat=8  
            print(f"'PHASE 7 {nom_machine}' : Message reçu: {message_reçu}")
            envoyer_message_liste(client_socket, trier_par_occurrences(convertir_en_tuples(messagePostShuffle2))) # Envoyer la liste triée par occurences
            envoyer_message(client_socket, "OK PHASE 7")
            print(f"'PHASE 7 {nom_machine}' : Message envoyé: OK PHASE 7")
            while message_reçu !="Kill":
                message_reçu = recevoir_message(client_socket)


        ### Fin du programme
        elif message_reçu == "Kill":
            etat=10
            #serveur_socket.close()
            sys.exit()  # Terminer le programme proprement
            
             
def gerer_phase_2(client_socket2, adresse_client):
    #print(f"'PHASE 2 {nom_machine}' : Gérer phase 2 pour {adresse_client}")
    # Recevoir des messages spécifiques dans une boucle
    while True:
        message_reçu = recevoir_message(client_socket2)
        #print(f"'PHASE 2 {nom_machine}' : Message reçu: {message_reçu} de {adresse_client}")
        message_reçu = json.loads(message_reçu)
        if etat < 6 :
            messagePostShuffle.append(message_reçu)
        elif etat >= 7:
            messagePostShuffle2.extend(message_reçu)

def accepter_connexion_phase1():
    while True:
        # Accepter une nouvelle connexion
        client_socket, adresse_client = serveur_socket.accept()
        # Créer un thread pour gérer la connexion
        thread_connexion = threading.Thread(target=gerer_connexion, args=(client_socket, adresse_client))
        thread_connexion.start()

def accepter_connexion_phase2():
    while True:
        # Accepter une nouvelle connexion
        client_socket2, adresse_client = serveur_socket2.accept()
        # Créer un thread pour gérer la connexion
        thread_connexion = threading.Thread(target=gerer_phase_2, args=(client_socket2, adresse_client))
        thread_connexion.start()

def fermer_connexion(client_socket):
    try:
        # Indiquer que vous avez terminé d'envoyer des données
        client_socket.shutdown(socket.SHUT_WR)
        # Recevoir les données restantes
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
        # Fermer le socket
        client_socket.close()
        print(f"Connexion fermée proprement : {client_socket}")
    except Exception as e:
        print(f"Erreur lors de la fermeture de la connexion: {e}")

# Fonction pour afficher la barre de progression
def afficher_barre_progression(iteration, total, texte):
    longueur = 50
    pourcentage = (iteration / total) * 100
    barre = '█' * int(longueur * iteration // total)
    espace = '-' * (longueur - len(barre))
    sys.stdout.write(f'\r{texte} |{barre}{espace}| {pourcentage:.2f}%')
    sys.stdout.flush()

# Fonction pour convertir une liste ('mot', occurance) en liste de tuples
def convertir_en_tuples(liste):
    tuples = []
    if len(liste) % 2 != 0:
        print("Attention : La liste contient un nombre impair d'éléments. Le dernier élément sera ignoré.")
        # Ignorer le dernier élément pour éviter l'IndexError
        liste = liste[:-1]
    for i in range(0, len(liste), 2):
        tuples.append((liste[i], liste[i + 1]))
    return tuples

# Fonction pour trier une liste de tuples par occurences
def trier_par_occurrences(liste_tuples):
    #return sorted(liste_tuples, key=lambda x: x[1], reverse=True)
    return sorted(liste_tuples, key=lambda x: (-x[1], x[0]))

# # Fonction pour répartir les mots par occurrences sur différentes machines. Mot avec 1 occurrence -> machine 1, mot avec 2 occurrences -> machine 2, etc.
# def repartir_mots_par_occurrences(liste_triee, occuranceMax, connexions):
#     #print(f"occuranceMax: {occuranceMax} // len(connexions): {len(connexions)}")
#     mots_par_machine = {machine: [] for machine in connexions.keys()}
#     segment = occuranceMax / len(connexions)  # Diviser les mots en segments en fonction du nombre de machines
#     for mot, occurrence in liste_triee:
#         # Calculer l'index de la machine en fonction de l'occurrence et du nombre de machines
#         machine_index = round((occurrence) / segment) - 1
#         machine = list(connexions.keys())[machine_index]
#         if mot : 
#             mots_par_machine[machine].extend((mot, occurrence))
    
#     return mots_par_machine

def repartir_mots_par_occurrences(liste_triee, occuranceMax, connexions):
    """Répartit les mots par occurrences sur différentes machines."""
    mots_par_machine = {machine: [] for machine in connexions.keys()}
    machines = list(connexions.keys())
    num_machines = len(machines)
    
    if num_machines == 1:
        # Si une seule machine, elle reçoit toute la liste triée
        mots_par_machine[machines[0]] = liste_triee
    elif num_machines == 2:
        # Si deux machines, la première reçoit les mots d'occurrence 1, la seconde reçoit tous les autres
        for mot, occurrence in liste_triee:
            if occurrence == 1:
                mots_par_machine[machines[0]].extend((mot, occurrence))
            else:
                mots_par_machine[machines[1]].extend((mot, occurrence))
    else:
        # Si plus de deux machines, répartir les mots selon les occurrences
        for mot, occurrence in liste_triee:
            if occurrence <= num_machines - 1:
                machine_index = occurrence - 1
            else:
                machine_index = num_machines - 1
            mots_par_machine[machines[machine_index]].extend((mot, occurrence))
    
    return mots_par_machine


# Fonction pour vérifier la mémoire disponible, si la mémoire est inférieure à 100 Mo, arrêter le programme
def memoire_disponible():
    while True:
        with open('/proc/meminfo', 'r') as f:
            meminfo = f.read()
        for line in meminfo.split('\n'):
            if 'MemAvailable:' in line:
                mem_disponible = int(line.split()[1]) / 1024  # Convertir en Mo
                if mem_disponible < 100:
                    print(f"'{nom_machine}' : Mémoire disponible trop faible: {mem_disponible:.2f} Mo")
                    print(f"'{nom_machine}' : Arrêt du programme!!!!! Merci de relancer celui-ci avec plus de machines")
                    sys.exit()
        time.sleep(5)  # Attendre 5 secondes avant de vérifier à nouveau

# Créer et démarrer le thread pour accepter les connexions
thread_accepter = threading.Thread(target=accepter_connexion_phase1)
thread_accepter.start()

# Créer et démarrer le thread pour vérifier la mémoire disponible
thread_memoire = threading.Thread(target=memoire_disponible)
thread_memoire.start()