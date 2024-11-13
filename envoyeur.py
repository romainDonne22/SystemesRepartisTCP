import socket
import json
import struct
import threading

# Lire les adresses des machines à partir du fichier machines.txt
with open('machines.txt', 'r') as file:
    machines = [line.strip() for line in file.readlines()]
# Convertir la liste des machines en JSON
machines_json = json.dumps(machines)

# Lire le nom des fichiers WET à partir du fichier fichiersWET.txt
with open('fichiersWET.txt', 'r') as file:
    #fichiersWET = [line.strip() for line in file.readlines()]
    fichiersWET = [line.strip() for line in file.readlines()[:3]] # On prend uniquement les 3 premiers fichiers WET pour tester

# Tableaux pour stocker les états de fin de chaque phase
tab_fin_phase_1 = [False]*len(machines)
tab_fin_phase_2 = [False]*len(machines)
tab_fin_phase_3 = [False]*len(machines)
tab_fin_phase_4 = [False]*len(machines)

# Les messages spécifiques à envoyer
# messages_specifiques = ["un", "bonjour", "depuis", "la", "lune", "la", "lune", "est", "belle"]

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
    #Envoyer la liste des machines à chaque machine
    for machine, client_socket in connexions.items():
        try:
            envoyer_message(client_socket, machines_json)
            print(f"Envoyé la liste des machines à {machine}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machine}: {e}")

    # Envoyer les messages spécifiques de manière cyclique
    # for index, message in enumerate(messages_specifiques):
    #     machine_index = index % len(machines)
    #     machine = machines[machine_index]
    #     try:
    #         client_socket = connexions[machine]
    #         envoyer_message(client_socket, message)
    #         print(f"Envoyé '{message}' à {machine}")
    #     except Exception as e:
    #         print(f"Erreur lors de l'envoi à {machine}: {e}")

    # Algo de split X
    # machine = machines[0]
    # for index, message in enumerate(messages_specifiques):
    #     if index == (0 or 1 or 2):
    #         machine = machines[0]
    #     elif index == (3 or 4):
    #         machine = machines[1]
    #     elif index == (5 or 6 or 7 or 8):
    #         machine = machines[2]
    #     try:
    #         client_socket = connexions[machine]
    #         envoyer_message(client_socket, message)
    #         print(f"Envoyé '{message}' à {machine}")
    #     except Exception as e:
    #         print(f"Erreur lors de l'envoi à {machine}: {e}")

    #Algo de split X pour envoyer les fichiers WET
    machine = machines[0]
    print(f"Envoi des noms des fichiers WET : ")
    for index, fchWET in enumerate(fichiersWET):
        if index == 0 :
            machine = machines[0]
        elif index == 1 :
            machine = machines[1]
        elif index == 2 :
            machine = machines[2]
        try:
            fichierWET_json = json.dumps(fchWET)
            client_socket = connexions[machine]
            envoyer_message(client_socket, fichierWET_json)
            print(f"Envoyé '{fchWET}' à {machine}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machine}: {e}")

    # Envoyer le message de fin de phase à chaque machine
    for machine, client_socket in connexions.items():
        try:
            envoyer_message(client_socket, "FIN PHASE 1")
            print(f"Envoyé 'FIN PHASE 1' à {machine}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machine}: {e}")

def recevoir_exactement(client_socket, n):
    data = b''
    while len(data) < n:
        packet = client_socket.recv(n - len(data))
        if not packet:
            raise ConnectionError("Connexion fermée par le client")
        data += packet
    return data

def recevoir_message(client_socket):
    # Recevoir la taille du message
    taille_message = struct.unpack('!I', recevoir_exactement(client_socket, 4))[0]
    # Recevoir le message en utilisant la taille
    data = recevoir_exactement(client_socket, taille_message)
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
    for machine, client_socket in connexions.items():
        try:
            message_reçu = recevoir_message(client_socket)
            if message_reçu == "OK FIN PHASE 1":
                print(f"Reçu '{message_reçu}' de {machine}")
                tab_fin_phase_1[machines.index(machine)] = True
                ######################## Phase 2 ########################
                if all(tab_fin_phase_1):
                    print("Toutes les machines ont fini la phase 1")
                    print("---------------------------------------")
                    for machine, client_socket in connexions.items():
                        envoyer_message(client_socket, "GO PHASE 2")
                        print(f"Envoyé 'GO PHASE 2' à {machine}")
                    message_reçu = recevoir_message(client_socket)
                    
                    if message_reçu == "OK PHASE 2":
                        for machine, client_socket in connexions.items():
                            print(f"Reçu '{message_reçu}' de {machine}")
                            tab_fin_phase_2[machines.index(machine)] = True
                        ######################## Phase 3 ########################
                        if all(tab_fin_phase_2):
                            print("Toutes les machines ont fini la phase 2")
                            print("---------------------------------------")
                            lancer_phase_3()
                            ######################## Phase 4 ########################
                            if all(tab_fin_phase_3):
                                print("Toutes les machines ont fini la phase 3")
                                print("---------------------------------------")
                                lancer_phase_4()
                                if all(tab_fin_phase_4):
                                    print("Toutes les machines ont fini la phase 4")
                                    print("---------------------------------------")
        except Exception as e:
            print(f"Erreur lors de la réception de {machine}: {e}")


def lancer_phase_3():
    for machine, client_socket in connexions.items():
        envoyer_message(client_socket, "GO PHASE 3")
        print(f"Envoyé 'GO PHASE 3' à {machine}")
        message_reçu = recevoir_message(client_socket)
        if message_reçu == "OK PHASE 3":
            for machine, client_socket in connexions.items():
                print(f"Reçu '{message_reçu}' de {machine}")
                tab_fin_phase_3[machines.index(machine)] = True
                  
def lancer_phase_4():
    for machine, client_socket in connexions.items():
        envoyer_message(client_socket, "GO PHASE 4")
        print(f"Envoyé 'GO PHASE 4' à {machine}")
    # Recevoir le REDUCE de chaque machine
    for machine, client_socket in connexions.items():
        message_recu = recevoir_message(client_socket)
        while message_recu == "OK PHASE 3":
            message_recu = recevoir_message(client_socket)
        #print(f"Reçu '{message_recu}' de {machine}")
        # Enregistrer la liste dans un fichier texte par machine
        with open(f'output/resultats_phase_4_{machine}.txt', 'w') as fichier:
            #for element in message_recu:
                #fichier.write(f"{element}\n")
            mots = message_recu.split(', "') # On sépare les mots après chaque virgule et guillemet - donc après la valeur
            fichier.write(f"{mots}\n") # On écrit les mots dans le fichier en sautant une ligne
        print(f"Liste enregistrée dans 'resultats_phase_4_{machine}.txt'")
    for machine, client_socket in connexions.items(): 
        message_reçu = recevoir_message(client_socket)
        if message_reçu == "OK PHASE 4":
            print(f"Reçu '{message_reçu}' de {machine}")
            tab_fin_phase_4[machines.index(machine)] = True

# Créer et démarrer les threads pour envoyer et recevoir les messages
thread_envoi = threading.Thread(target=envoyer_messages)
thread_reception = threading.Thread(target=recevoir_messages)

thread_envoi.start()
thread_reception.start()

# Attendre que les threads se terminent
thread_envoi.join()
thread_reception.join()