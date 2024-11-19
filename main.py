from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace
import time


def word_count_from_file(file_path, num_results=20, sort_order="none"):
    # Créer une session Spark
    spark = SparkSession.builder.\
                            master('local').\
                            appName("WordCount").\
                            config("spark.ui.port", "4040").\
                            getOrCreate()
    
    # Lire les données à partir du fichier texte
    text_data = spark.read.text(file_path)
    
    # Nettoyer les données (supprimer les caractères spéciaux)
    cleaned_data = text_data.withColumn('cleaned_value', regexp_replace(text_data['value'], r'[^a-zA-Z0-9\s]', ''))
    # Séparer le texte en mots
    words = cleaned_data.select(explode(split(cleaned_data['cleaned_value'], r'\s+')).alias("word"))
    word_count = words.groupBy("word").count()
    
    # Appliquer le tri en fonction de l'option choisie
    if sort_order == "asc":
        word_count = word_count.orderBy("count", ascending=True)
    elif sort_order == "desc":
        word_count = word_count.orderBy("count", ascending=False)
    
    # Obtenir le nombre total de lignes dans le DataFrame
    total_count = word_count.count()
    # Ajuster le nombre de résultats à afficher si nécessaire
    num_results = min(num_results, total_count)
    word_count.show(num_results)
    
    print("Accéder à Spark UI sur : http://localhost:4040")
    time.sleep(300)
    
    spark.stop()


def get_file(prompt, default_value="text_files/text_test.txt"):
    while True:
        file_input = input(prompt).strip()
        if file_input == "":
            return default_value
        if file_input.endswith(".txt"):
            return file_input
        print("\nErreur : Le fichier doit se terminer par '.txt'. Essayez à nouveau.")


def get_int(prompt, default_value=20):
    while True:
        user_input = input(prompt).strip()
        if user_input == "":
            return default_value
        try:
            return int(user_input)
        except ValueError:
            print("\nErreur : Veuillez entrer un nombre entier valide.")


def get_sort(prompt):
    valid_inputs = {"asc", "desc"}
    while True:
        sort_input = input(prompt).strip().lower()
        if sort_input == "":
            return "none"
        if sort_input in valid_inputs:
            return sort_input
        print("\nErreur : Veuillez entrer 'asc', 'desc' ou laisser vide pour ne pas trier.")



if __name__ == "__main__":
    file_path = get_file("\n\nEntrez votre fichier .txt.\nVous pouvez laisser vide pour utiliser 'text_test.txt', ou choisir text_files/test2.txt : ", "text_files/text_test.txt")
    num_results = get_int("\n\nCombien de mots voulez-vous afficher ? (laissez vide pour 20) : ", 20)
    sort_order = get_sort("\n\nSouhaitez-vous trier par nombre d'occurrences ? ('asc', 'desc', ou vide pour aucun tri) : ")
    word_count_from_file(file_path, num_results, sort_order)
