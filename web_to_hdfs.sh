hdfs dfs -mkdir /projet

fichier=/projet/taxiride

URL=http://training.ververica.com/trainingData/nycTaxiRides.gz

wget -nv -O - $URL | gunzip | hdfs dfs -put - ${fichier}


fichier=/projet/taxiFares
URL=http://training.ververica.com/trainingData/nycTaxiFares.gz


wget -nv -O - $URL | gunzip | hdfs dfs -put - ${fichier}

hdfs dfs -chown hdfs:hdfs /
