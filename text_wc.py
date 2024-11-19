from pyspark.sql import SparkSession
import time

# Créer une session Spark
spark = SparkSession.builder \
    .appName("WordCount") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# Lire un fichier texte
# Assure-toi que le fichier texte est dans le même répertoire ou utilise un chemin absolu
fichier_texte = "text_files/reduit_fichier_texte.txt"
rdd = spark.sparkContext.textFile(fichier_texte)

# Transformation : Filtrer les lignes contenant un mot-clé (par exemple, "Spark")
mot_cle = "Spark"
lignes_filtrees = rdd.filter(lambda ligne: mot_cle in ligne)

# Transformation : Compter le nombre de mots dans chaque ligne
lignes_comptees = lignes_filtrees.map(lambda ligne: (ligne, len(ligne.split())))

# Action : Collecter les résultats
resultats = lignes_comptees.collect()

# Afficher les résultats
print("Résultats du traitement :")
for ligne, nombre_mots in resultats:
    print(f"Ligne : '{ligne}' - Nombre de mots : {nombre_mots}")

# Garder l'application active pour consulter Spark UI
print("Accéder à Spark UI sur : http://localhost:8080")
time.sleep(300)  # Laisse l'application tourner pendant 5 minutes

# Arrêter Spark
spark.stop()
