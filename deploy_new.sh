#!/bin/bash
login="donne-24"
localFolder="./"
todeploy="dossierAdeployer"
remoteFolder="BGD701"
nameOfTheScript="script.py"
#create a machines.txt file with the list of computers
computers=($(cat machines.txt))
# computers=("tp-1a207-34" "tp-1a207-35" "tp-1a207-37")

# Définir la commande ssh pour exécuter la commande à distance
command1=("ssh" "-tt" "$login@${computers[0]}" "rm -rf $remoteFolder; mkdir $remoteFolder;wait;")

# Exécuter la commande ssh sur la première machine
echo ${command1[*]}
"${command1[@]}";wait;

command2=("scp" "-r" "$localFolder$todeploy" "$login@${computers[0]}:$remoteFolder")
echo ${command2[*]}
"${command2[@]}";wait;

# Supprimer le fichier fichiersWET.txt s'il existe
if [ -f "fichiersWET.txt" ]; then
    rm "fichiersWET.txt"
fi

# Récupérer les noms des fichiers présents dans le dossier sur la machine distante et écrire le résultat dans le fichier local fichiersWET.txt
remoteFolderPath="/cal/commoncrawl/"
outputFile="fichiersWET.txt"
command4=("ssh" "$login@${computers[0]}" "ls -p $remoteFolderPath | grep -v /")
echo ${command4[*]}
"${command4[@]}" > $outputFile;wait;

for c in ${computers[@]}; do
  #this command goes to the remote folder, waits 3 seconds and executes script
  command3=("ssh" "-tt" "$login@$c" "cd $remoteFolder/$todeploy; python3 $nameOfTheScript; wait;")

  echo ${command3[*]}
  "${command3[@]}" &
done


