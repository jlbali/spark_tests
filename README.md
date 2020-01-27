# spark_tests
Tests with Spark on Python

Installatión:

- pipenv:

pip install pipenv

Try installing one package at a time from the Pipenv if it gets stuck on "locking". Comment the lines in the Pipfile (TOML format) with #.

- pyspark

pipenv install pyspark

No longer necessary the previous Spark installation steps.

- PyCharm

Directly open the project with the Pipfile present, it will interpret it correctly as a Pipenv
project. Do not use "New Project".

If PyCharm asks for an interpreter in a yellow sign, mark use Pipenv interpreter.

PyCharm needs the main Pipenv file and python project in the main directory of the Github project, as far as I could tell.


Issues:

- Cannot install Spyder through pipenv. Strange error.

- Hint Typing: look how to declare types for variables.

- Error "Unsopported class file major version 56" (or 55)


Fix: install Java 1.8 along with your other versions,

sudo apt install openjdk-8-jdk

sudo update-alternatives --config java
(select java 8 and confirm changes)

java --version
(check which version of Java is running)

Source: https://stackoverflow.com/questions/53583199/pyspark-error-unsupported-class-file-major-version-55






