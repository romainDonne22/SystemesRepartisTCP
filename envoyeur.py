import socket
import json
import struct
import threading
import time # Pour mesurer le temps d'exécution pour la loi d'Amdahl de façon empirique
import sys

def envoyer_message(client_socket, message):
    # Convertir le message en bytes
    message_bytes = message.encode('utf-8')
    # Envoyer la taille du message en utilisant send
    taille_message = struct.pack('!I', len(message_bytes))
    total_envoye = 0
    while total_envoye < len(taille_message):
        envoye = client_socket.send(taille_message[total_envoye:])
        if envoye == 0:
            raise RuntimeError("La connexion a été fermée")
        total_envoye += envoye
    # Envoyer le message
    client_socket.sendall(message_bytes)

def envoyer_messages():
    global start_time #variable pour enregistrer le temps de début d'execution
    start_time = time.time()
    #Envoyer la liste des machines à chaque machine
    for machine, client_socket in connexions.items():
        try:
            envoyer_message(client_socket, machines_json)
            print(f"Envoyé la liste des machines à {machine}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machine}: {e}")

    #Algo de split pour envoyer les fichiers WET aux différentes machines
    machine = machines[0]
    repartition = {machine: [] for machine in machines} # Dictionnaire pour stocker les fichiers assignés à chaque machine
    for index, fchWET in enumerate(fichiersWET):
        machine = machines[index % len(machines)]
        repartition[machine].append(fchWET)

    # Envoyer les fichiers WET à chaque machine
    for machine, client_socket in connexions.items():
        try:
            fichierWET_json = json.dumps(repartition[machine])
            envoyer_message(client_socket, fichierWET_json)
            print(f"Envoyé {len(repartition[machine])} fichiers WET à {machine}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machine}: {e}")

    # Envoyer le message de fin de phase à chaque machine
    for machine, client_socket in connexions.items():
        try:
            envoyer_message(client_socket, "FIN PHASE 1")
            print(f"Envoyé 'FIN PHASE 1' à {machine}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machine}: {e}")

# fonction pour divise une liste en n sous-listes de taille égale
def diviser_liste_uniformement(lst, n):
    print(f"Il y a {len(lst)} mots à partager vers {n} machines")
    # Vérifier si la liste a un nombre impair d'éléments
    if len(lst) % 2 != 0:
        print(f"Il y a une erreur la liste doit être paire")  # Ajouter un élément fictif pour rendre la liste de longueur paire
        return None
    taille = len(lst) // n # Calculer la taille des sous-listes // pour la division entière
    # S'assurer que la taille des sous-listes est paire
    if taille % 2 != 0:
        taille += 1
    # Diviser la liste en sous-listes de taille égale jusqu'à l'avant-dernier élément
    sous_listes = [lst[i * taille:(i + 1) * taille] for i in range(n - 1)]
    # Ajouter le complément restant à la dernière sous-liste
    sous_listes.append(lst[(n - 1) * taille:])
    return sous_listes

#Divise une liste en morceaux de taille maximale spécifiée."""
def diviser_liste(lst, taille_max):
    return [lst[i:i + taille_max] for i in range(0, len(lst), taille_max)]

def recevoir_exactement(client_socket, n):
    data = b''
    while len(data) < n:
        try:
            packet = client_socket.recv(n - len(data))
            if not packet:
                raise ConnectionError("Connexion fermée par le client")
            data += packet
        except ConnectionError as e:
            print(f"Erreur de connexion: {e}")
            return None
        except socket.timeout:
            print("Erreur: Timeout lors de la réception des données")
            return None
        except Exception as e:
            print(f"Erreur lors de la réception des données: {e}")
            return None
    return data

def recevoir_message(client_socket):
    # Recevoir la taille du message
    taille_message = struct.unpack('!I', recevoir_exactement(client_socket, 4))[0]
    # Recevoir le message en utilisant la taille
    data = recevoir_exactement(client_socket, taille_message)
    # Réinitialiser le timeout à None pour désactiver le timeout
    return data.decode('utf-8')

def recevoir_message_dict(client_socket):
    try:
        # Recevoir la taille du message (supposons qu'elle soit envoyée sur 4 octets)
        taille_message = client_socket.recv(4)
        if not taille_message:
            raise ConnectionError("Connexion fermée lors de la réception de la taille du message.")
        taille_message = int.from_bytes(taille_message, byteorder='big')
        # Recevoir le message complet
        data = recevoir_exactement(client_socket, taille_message)
        if not data:
            raise ConnectionError("Connexion fermée lors de la réception du message.")
        # Convertir la chaîne JSON en dictionnaire
        message_dict = json.loads(data.decode('utf-8'))
        return message_dict
    except Exception as e:
        print(f"Erreur lors de la réception du message: {e}")
        return None

def recevoir_messages():
    try:
        while True:
            if not all(tab_fin_phase_1):
                lancer_phase_1()
                print("Toutes les machines ont fini la phase 1")
                print("---------------------------------------")
            ######################## Phase 2 ########################
            elif all(tab_fin_phase_1) and not all(tab_fin_phase_2):
                lancer_phase_2()
                print("Toutes les machines ont fini la phase 2")
                print("---------------------------------------")
            ######################## Phase 3 ########################
            elif all(tab_fin_phase_2) and not all(tab_fin_phase_3):
                lancer_phase_3()
                print("Toutes les machines ont fini la phase 3")
                print("---------------------------------------")
            ######################## Phase 4 ########################
            elif all(tab_fin_phase_3) and not all(tab_fin_phase_4):
                mots=lancer_phase_4()
                print("Toutes les machines ont fini la phase 4")
                print("---------------------------------------")
            ######################## Phase 5 ########################
            elif all(tab_fin_phase_4) and not all(tab_fin_phase_5) and nbMAPREDUCE == 2:
                lancer_phase_5(mots)
                mots.clear() # Vider la liste de mots pour gagner de la mémoire
                print("Toutes les machines ont fini la phase 5")
                print("---------------------------------------")
            ######################## Phase 6 ########################
            elif all(tab_fin_phase_5) and not all(tab_fin_phase_6) and nbMAPREDUCE == 2:
                lancer_phase_6()
                print("Toutes les machines ont fini la phase 6")
                print("---------------------------------------")
            ######################## Phase 7 ########################
            elif all(tab_fin_phase_6) and not all(tab_fin_phase_7) and nbMAPREDUCE == 2:
                lancer_phase_7()
                print("Toutes les machines ont fini la phase 7")
                print("---------------------------------------")
            ######################## Statistiques ###################
            elif all(tab_fin_phase_7) or nbMAPREDUCE == 1:
                lancer_fin_programme()
    except Exception as e:
        print(f"Erreur lors de la réception de {machine}: {e}")




def lancer_phase_1():
    for machine, client_socket in connexions.items():
        message_reçu = recevoir_message(client_socket)
        if message_reçu == "OK FIN PHASE 1":
            print(f"Reçu '{message_reçu}' de {machine}")
            tab_fin_phase_1[machines.index(machine)] = True

def lancer_phase_2():
    for machine, client_socket in connexions.items():
        envoyer_message(client_socket, "GO PHASE 2")
        print(f"Envoyé 'GO PHASE 2' à {machine}")
    for machine, client_socket in connexions.items():
        message_reçu = recevoir_message(client_socket)
        if message_reçu == "OK PHASE 2":
            print(f"Reçu '{message_reçu}' de {machine}")
            tab_fin_phase_2[machines.index(machine)] = True

def lancer_phase_3():
    for machine, client_socket in connexions.items():
        envoyer_message(client_socket, "GO PHASE 3")
        print(f"Envoyé 'GO PHASE 3' à {machine}")
    for machine, client_socket in connexions.items():
        message_reçu = recevoir_message(client_socket)
        if message_reçu == "OK PHASE 3":
            print(f"Reçu '{message_reçu}' de {machine}")
            tab_fin_phase_3[machines.index(machine)] = True
                  
def lancer_phase_4():
    mots=[] # Liste pour stocker les mots
    Tousmots=[] # Liste pour stocker tous les mots
    for machine, client_socket in connexions.items():
        envoyer_message(client_socket, "GO PHASE 4")
        print(f"Envoyé 'GO PHASE 4' à {machine}")
    # Recevoir le REDUCE de chaque machine
    for machine, client_socket in connexions.items():
        message_recu = recevoir_message(client_socket)
        mots = json.loads(message_recu)
        Tousmots.extend(mots) # Ajouter les mots reçus à la liste
        if nbMAPREDUCE == 1: # Si on a un seul MAPREDUCE à effectuer      
            ecrire_fichier(mots, machine, "phase_4")
    for machine, client_socket in connexions.items(): 
        message_reçu = recevoir_message(client_socket)
        if message_reçu == "OK PHASE 4":
            print(f"Reçu '{message_reçu}' de {machine}")
            tab_fin_phase_4[machines.index(machine)] = True
    return Tousmots

def lancer_phase_5(mots):
    for machine, client_socket in connexions.items():
        envoyer_message(client_socket, "GO PHASE 5")
        print(f"Envoyé 'GO PHASE 5' à {machine}")
    # Diviser la liste de mots en sous-listes de taille égale
    sous_listes = diviser_liste_uniformement(mots, len(machines))
    # Envoyer chaque sous-liste à une machine différente en utilisant un modulo
    for i, (machine, client_socket) in enumerate(connexions.items()):
        sous_liste = sous_listes[i]
        mots_json = json.dumps(sous_liste)
        print(f"Envoyé {len(sous_liste)} mots à {machine}")
        envoyer_message(client_socket, mots_json)
    occurrence=[0]*len(machines)
    i=0
    for machine, client_socket in connexions.items(): 
        occurrence[i] = recevoir_message(client_socket)
        print(f"Reçu 'Le mot le plus fréquent a une occurrence de : {occurrence[i]} de {machine}")
        i=i+1
    occurrenceMax = max(occurrence)
    for machine, client_socket in connexions.items(): 
        envoyer_message(client_socket, occurrenceMax)
    for machine, client_socket in connexions.items(): 
        message_reçu = recevoir_message(client_socket)
        if message_reçu == "OK PHASE 5":
            print(f"Reçu '{message_reçu}' de {machine}")
            tab_fin_phase_5[machines.index(machine)] = True

def lancer_phase_6():
    for machine, client_socket in connexions.items():
        envoyer_message(client_socket, "GO PHASE 6")
        print(f"Envoyé 'GO PHASE 6' à {machine}")
    for machine, client_socket in connexions.items():
        message_reçu = recevoir_message(client_socket)
        if message_reçu == "OK PHASE 6":
            print(f"Reçu '{message_reçu}' de {machine}")
            tab_fin_phase_6[machines.index(machine)] = True

def lancer_phase_7():
    for machine, client_socket in connexions.items():
        envoyer_message(client_socket, "GO PHASE 7")
        print(f"Envoyé 'GO PHASE 7' à {machine}")
    # Recevoir le REDUCE 2 de chaque machine
    for machine, client_socket in connexions.items():
        message_recu = recevoir_message(client_socket)
        mots = json.loads(message_recu)        
        ecrire_fichier(mots, machine, "phase_7")
        
    for machine, client_socket in connexions.items():
        message_reçu = recevoir_message(client_socket)
        if message_reçu == "OK PHASE 7":
            print(f"Reçu '{message_reçu}' de {machine}")
            tab_fin_phase_7[machines.index(machine)] = True
    
def ecrire_fichier(mots, machine, phase):
    taille_max = 5000000  # Taille maximale de chaque fichier (mettre un nombre paire)
    morceaux = []
    if len(mots) > taille_max:
        morceaux = diviser_liste(mots, taille_max)
        # Enregistrer chaque morceau dans un fichier csv séparé
        for i, morceau in enumerate(morceaux):
            with open(f'output/resultats_{phase}_{machine}_part{i + 1}.csv', 'w') as fichier:
                fichier.write(f"{morceau}")  # On écrit les mots dans le fichier
            print(f"Liste enregistrée dans 'resultats_{phase}_{machine}_part{i + 1}.csv'")
    else:
        # Enregistrer la liste dans un seul fichier csv
        with open(f'output/resultats_phase_7_{machine}.csv', 'w') as fichier:
            fichier.write(f"{mots}")
        print(f"Liste enregistrée dans 'resultats_phase_7_{machine}.csv'")

def lancer_fin_programme():
    for machine, client_socket in connexions.items():
        envoyer_message(client_socket, "Kill")
        print(f"Envoyé 'Kill' à {machine}")
    end_time = time.time()
    execution_time = end_time - start_time # Calcul du temps d'exécution
    minutes = int(execution_time // 60) # On divise le temps total par 60 pour avoir les minutes
    seconds = int(execution_time % 60) # On fait un modulo de 60 pour avoir les secondes restantes
    print(f"Temps d'exécution : {minutes} minutes et {seconds} secondes")
    print(f"Soit en secondes : {int(execution_time)} secondes")
    print(f"Nombre de fichiers WET : {nbfichiers}")
    print(f"Nombre de machines : {nbmachines}")
    print(f"Nombre de MAPREDUCE : {nbMAPREDUCE}")
    print("Fin du programme")
    time.sleep(3) # Attendre avant de terminer le programme
    sys.exit()


# L'utilisateur renseigne le nombre de fichiers WET à traiter et le nombre de machines à utiliser et le nombre de MAPREDUCE
while True:
    try:
        nbfichiers = int(input("Entrez le nombre de fichiers (entre 1 et 100) : "))
        if nbfichiers < 1 or nbfichiers > 100:
            raise ValueError
        break  # Sortir de la boucle si la valeur est valide
    except ValueError as e:
        print(f"Erreur de saisie : {e}. Le nombre de fichiers doit être un entier compris entre 1 et 100.")

while True:
    try:
        nbmachines = int(input("Nombre de machines à utiliser ? [Entrez un nombre entier, 30 au maximum] : "))
        if nbmachines <1 or nbmachines >30 :
            raise ValueError
        break  # Sortir de la boucle si la valeur est valide
    except ValueError as e:
        print(f"Erreur de saisie : {e}. Le nombre de machines doit être un entier compris entre 1 et 30.")

while True:
    try:
        nbMAPREDUCE = int(input("Nombre de MAPREDUCE à effectuer ? [Entrez '1' pour fire le WordCount ou '2' pour faire le trie en plus] : "))
        if not (nbMAPREDUCE==1 or nbMAPREDUCE==2) :
            raise ValueError
        break  # Sortir de la boucle si la valeur est valide
    except ValueError as e:
        print(f"Erreur de saisie : {e}. Entrez '1' pour fire le WordCount ou '2' pour faire le trie en plus.")

# Lire les adresses des machines à partir du fichier machines.txt
with open('machines.txt', 'r') as file:
    machines = [line.strip() for line in file.readlines()[:nbmachines]]
machines_json = json.dumps(machines) # Convertir la liste des machines en JSON

# Lire le nom des fichiers WET à partir du fichier fichiersWET.txt
with open('fichiersWET.txt', 'r') as file:
    fichiersWET = [line.strip() for line in file.readlines()[:nbfichiers]] 

# Tableaux pour stocker les états de fin de chaque phase
tab_fin_phase_1 = [False]*len(machines)
tab_fin_phase_2 = [False]*len(machines)
tab_fin_phase_3 = [False]*len(machines)
tab_fin_phase_4 = [False]*len(machines)
tab_fin_phase_5 = [False]*len(machines)
tab_fin_phase_6 = [False]*len(machines)
tab_fin_phase_7 = [False]*len(machines)

# Dictionnaire pour stocker les connexions
connexions = {}

# Créer les connexions à toutes les machines
for machine in machines:
    try:
        # Créer un socket TCP/IP
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Se connecter à la machine
        client_socket.connect((machine, 4444))
        # Stocker la connexion
        connexions[machine] = client_socket
        print(f"Connexion établie avec {machine}")
    except Exception as e:
        print(f"Erreur lors de la connexion à {machine}: {e}")

# Créer et démarrer les threads pour envoyer et recevoir les messages
thread_envoi = threading.Thread(target=envoyer_messages)
thread_reception = threading.Thread(target=recevoir_messages)
thread_envoi.start()
thread_reception.start()

# Attendre que les threads se terminent
thread_envoi.join()
thread_reception.join()