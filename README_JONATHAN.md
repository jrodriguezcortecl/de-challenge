# Data Engineer - Challenge

A continuación detallaré en términos generales los pasos realizados:

## Proceso ETL

- Durante el proceso ETL verifiqué la existencia o no de las tablas donde se insertaría la Base de Datos, procediendo a crearlas en caso de ser necesario
- Se procedió a realizar la extracción de la data y realizar las transformaciones necesarias. Se tomó la decisión de modificar el campo de fecha y adaptarlo al formato de base de datos
- Se realizaron las consultas necesarias las cuales se encuentran impresas al ejecutar el código
- Finalmente, se realizó el proceso de guardado en las tablas correspondientes

- En la ejecución del código se hace el llamado de credenciales de la base de datos, se debe crear el archivo database.ini para colocar la información, para ello existe el archivo de referencia database_sample.ini

## Data model
- Se procedió a establecer la asociación de las tablas en 3FN y se guardó el archivo con el modelo resultante

## Deployment
- Se incluyó un archivo deploy.sh para ejecutar el archivo, también se creó un archivo dentro de .git/hooks/ llamado post-commit para que cada vez que se realice el commit, se ejecute el archivo.sh (asumiendo que nos encontramos en un entorno de desarrollo para la rama)
