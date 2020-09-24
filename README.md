# Exam project 
**Tommaso Bombardi (904212)** e 
**Maria Maddalena Mascellaro (904213)**

Big Data course (81932), University of Bologna.

# MapReduce
- Compilare il progetto nella cartella `mapreduce` con il comando `.\gradlew` per ottenere il jar
- Copiare il jar __BDE-mr-bombardi-mascellaro__ nel cluster, e lanciare il seguente comando per eseguire la catena dei job:
`hadoop jar BDE-mr-bombardi-mascellaro.jar BdeMapReduce /user/mmascellaro/project/movies.csv
 /user/mmascellaro/project/ratings.csv /user/mmascellaro/project/tags.csv 
 /user/mmascellaro/outputMapReduce 3.5 10 4 2 2 2 2 2`
    - I primi 3 argomenti sono i percorsi delle tabelle `Movies`, `Ratings` e `Tags` contenuti nella cartella hdfs `project`
    - Il 4° argomento è la cartella hdfs che conterrà l'output e che verrà suddivisa automaticamente in due sotto-directory (`query1` e `query2`). Al loro interno saranno contenuti gli output di ogni stage del job
    - Il 5° argomento è, nella prima query, la soglia che indica la minima valutazione media dei film considerati (valore che può variare da 0.5 a 5)
    - Il 6° argomento è, nella seconda query, il numero di film da visualizzare per ogni anno
    - Gli argomenti 7, 8 e 9 sono i numeri di reducer da assegnare ai primi 3 stage della prima query (al quarto stage è assegnato sempre un solo reducer)
    - Gli argomenti 10, 11 e 12 sono i numeri di reducer da assegnare ai primi 3 stage della seconda query (al quarto stage è assegnato sempre un solo reducer)
    - Gli ultimi sei argomenti sono opzionali e, se non specificati, MapReduce usa il numero di reducer previsti di default
    - Se si vuole specificare solo il numero di reducer relativi ad alcuni stage, è possibile inserire 0 negli stage rimanenti per usare i valori di default

# Spark e Spark SQL
- Compilare il progetto nella cartella `spark` con il comando `.\gradlew` per ottenere il jar
- Copiare il jar __BDE-spark-bombardi-mascellaro__ nel cluster, e lanciare il seguente comando
    - __Spark__: `spark2-submit --executor-cores 3 --num-executors 2 --executor-memory 2G --class BdeSpark BDE-spark-bombardi-mascellaro.jar /user/mmascellaro/project/movies.csv /user/mmascellaro/project/ratings.csv /user/mmascellaro/project/tags.csv /user/mmascellaro/outputSpark 3.5 10`
    - __SparkSQL__: `spark2-submit --executor-cores 3 --num-executors 2 --executor-memory 2G --class BdeSparkSQL BDE-spark-bombardi-mascellaro.jar /user/mmascellaro/project/movies.csv /user/mmascellaro/project/ratings.csv /user/mmascellaro/project/tags.csv /user/mmascellaro/outputSparkSQL 3.5 10`
- Le istruzioni seguenti valgono per entrambe le implementazioni 
    - I primi 3 argomenti sono i percorsi delle tabelle `Movies`, `Ratings` e `Tags` contenuti nella cartella hdfs `project`
    - Il 4° argomento è la cartella hdfs che conterrà l'output e che verrà suddivisa automaticamente in due sotto-directory (`query1` e `query2`)
    - Il 5° argomento è, nella prima query, la soglia che indica la minima valutazione media dei film considerati (valore che può variare da 0.5 a 5)
    - Il 6° argomento è, nella seconda query, il numero di film da visualizzare per ogni anno
- Per quanto riguarda la configurazione del job, sono stati scelti i seguenti parametri
    - --executor-cores 3 
    - --num-executors 2 
    - --executor-memory 2G
