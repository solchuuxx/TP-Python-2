import os
import csv
import sys
import MySQLdb
# Conexion base de datos
try:
    db = MySQLdb.connect("localhost","root","","localidades")
except MySQLdb.Error as e:
    print("No se pudo conectar a la base de datos:",e)
    sys.exit(1)
print("Se conectó la base de datos")
cursor = db.cursor()
# borrar la tabla si ya existe
cursor.execute("DROP TABLE IF EXISTS localidades")
print("Tabla 'localidades' eliminada.")
cursor.execute("""

            CREATE TABLE localidades (
            provincia VARCHAR(100),
            id INT,
            localidad VARCHAR(100),
            cp INT,
            id_prov_mstr INT
        )
        """)
print("Tabla 'localidades' creada exitosamente.")

#insertar los datos en la tabla
with open('localidades.csv', 'r') as file:
    reader = csv.reader(file, delimiter=',', quotechar='"')
    next(reader)
    localidades = [(fila[0], fila[1], fila[2], fila[3], fila[4]) for fila in reader if len(fila) <= 5]

cursor.executemany("INSERT INTO localidades (provincia, id, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s);", localidades)

ruta_carpeta = "/archivoscsv"

lista_provincias = ["Formosa", "Córdoba", "Neuquen","Buenos Aires","Catamarca", "Santa Fe", "Corrientes", "Chaco", "Chubut", "Entre Rios", "Jujuy", "Salta", "La Pampa", "La Rioja", "Mendoza", "Misiones", "Rio Negro", "Salta", "San Juan", "San Luis", "Santa Cruz", "Santiago del Estero", "Tucuman", "Tierra del Fuego", "Ciudad Autonoma de Buenos Aires"]

for provincia in lista_provincias:
    cursor.execute("SELECT provincia, GROUP_CONCAT(localidad) as localidad, id, cp, id_prov_mstr FROM localidades WHERE provincia = %s GROUP BY provincia, id, cp, id_prov_mstr", (provincia,))
    sql = cursor.fetchall()
    os.makedirs(ruta_carpeta, exist_ok=True)
    nombre_archivo = os.path.join(ruta_carpeta, provincia+".csv")
    with open(nombre_archivo, mode='w', newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(sql)

print("Datos cargados correctamente")

db.commit()

db.close()


