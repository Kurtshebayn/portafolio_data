En este reto, deberás procesar registros de vuelos en formato **JSON newlines delimited**, aplicando Programación Orientada a Objetos (POO) y generando un archivo final en Parquet. La idea es que crees tres clases para manejar las etapas de extracción, transformación y guardado de la información.

> Nota:
> Un archivo Parquet es un formato de almacenamiento de datos columnar, diseñado para manejar grandes volúmenes de información de manera eficiente. Al trabajar con columnas, no solo permite una compresión y un procesamiento más rápidos en comparación con formatos basados en filas (como CSV), sino que también almacena los tipos de datos de cada columna, lo que facilita tanto la optimización como la integridad de la información. 

Enlace: https://drive.google.com/file/d/1phVKlbOB9dDLZjLxa1fOGP5UZen2Yh8A/view?usp=share_link

---

## Diccionario de datos
Cada línea del archivo de entrada contiene un objeto JSON con la siguiente estructura (ejemplo de registro):

```json

{"FL_DATE": "2006-01-01", "DEP_DELAY": 5, "ARR_DELAY": 19, "AIR_TIME": 350, "DISTANCE": 2475, "DEP_TIME": 9.083333, "ARR_TIME": 12.483334}
{"FL_DATE": "2006-01-02", "DEP_DELAY": 167, "ARR_DELAY": 216, "AIR_TIME": 343, "DISTANCE": 2475, "DEP_TIME": 11.783334, "ARR_TIME": 15.766666}
{"FL_DATE": "2006-01-03", "DEP_DELAY": -7, "ARR_DELAY": -2, "AIR_TIME": 344, "DISTANCE": 2475, "DEP_TIME": 8.883333, "ARR_TIME": 12.133333}
```

donde

- **FL_DATE**: Fecha del vuelo.
- **DEP_DELAY**: Retraso de despegue (en minutos).
- **ARR_DELAY**: Retraso de aterrizaje (en minutos).
- **AIR_TIME**: Tiempo total en el aire (en minutos).
- **DISTANCE**: Distancia total recorrida (en millas).
- **DEP_TIME**: Hora de despegue en formato decimal (por ejemplo, `9.083333` para ~9:05).
- **ARR_TIME**: Hora de aterrizaje en formato decimal (por ejemplo, `23.483334` para ~23:29).

---
## Clases a implementar


1. **Clase de Extracción**  
   - Leer los registros en formato JSON newlines delimited.  
   - Almacenar o retornar estos datos como una estructura de Python (lista de diccionarios).

2. **Clase de Transformación**  
   - Aplicar las **transformaciones** necesarias, renombrar campos y generar las nuevas columnas que se detallan más adelante.  
   - **No** se explicará el procedimiento de cómo hacer cada transformación, solo lo que se espera obtener.

3. **Clase de Guardado**  
   - Guardar los datos resultantes en formato **Parquet**.  
   - Se debe forzar un **esquema** con tipos de datos específicos para cada columna.

> #### Nota
> ##### Puedes implementar más clases si así lo consideras necesario.

---

## Transformaciones esperadas

1. **Conversión de campos**  
   - Se espera que conviertas `FL_DATE` en un objeto `datetime`.
   - `DEP_TIME` debe estar en formato %H:%M.
   - `ARR_TIME` debe estar en formato %H:%M.


2. **Creación de **nuevas** variables derivadas**  
   - `flight_datetime` en formato datetime que represente la fecha y hora de despegue del vuelo.
   - `average_speed` de cada vuelo, calculada como `DISTANCE / (AIR_TIME / 60)` (millas por hora).
   - `total_delay` como la suma de `DEP_DELAY` y `ARR_DELAY`.
   - `on_time` como un indicador booleano (true/false) de si el vuelo llegó a tiempo co con retraso total.
   - `day_of_week` como el día de la semana del vuelo.
   - `day_of_month` como el día del mes del vuelo.
   - `month` como mes del vuelo.

3. **Renombrar campos**  
     - `FL_DATE` → `flight_date`
     - `DEP_DELAY` → `departure_delay`
     - `ARR_DELAY` → `arrival_delay`
     - `AIR_TIME` → `air_time_minutes`
     - `DISTANCE` → `distance_miles`
     - `DEP_TIME` → `departure_time_decimal`
     - `ARR_TIME` → `arrival_time_decimal`

---
## Esquema de datos del parquet

El archivo Parquet final deberá poseer estos campos con los tipos indicados:

- `flight_date`: datetime
- `departure_delay`: int16
- `arrival_delay`: int16
- `air_time_minutes`: int16
- `distance_miles`: int32
- `departure_time_decimal`: str
- `arrival_time_decimal`: str
- `flight_datetime`: datetime
- `average_speed`: float64
- `total_delay`: int16
- `on_time`: bool
- `day_of_week`: int16
- `day_of_month`: int16
- `month`: int16

---

## Archivo `main.py`

Además de las tres clases, deberás crear un archivo **`main.py`** cuyo propósito es **controlar** el flujo completo del programa. Debe:

1. **Instanciar** la clase de extracción para leer los datos de vuelos.  
2. **Instanciar** la clase de transformación para generar los campos y renombrar adecuadamente cada columna.  
3. **Instanciar** la clase de guardado para exportar el resultado final en formato Parquet.  

El archivo **`main.py`** no debe contener lógica de transformación o lectura dentro de él; simplemente hace llamadas a los métodos correspondientes de cada clase.


---

## Estructura de archivos sugerida

La carpeta del proyecto podría organizarse así:

```
├── extraction.py      (clase de Extracción)
├── transform.py       (clase de Transformación)
├── load.py            (clase de Guardado)
├── main.py            (punto de entrada principal del programa)
└── README.md          (documentación)
```

- **`extraction.py`**: define la clase encargada de leer los datos.  
- **`transform.py`**: define la clase que aplica las transformaciones requeridas.  
- **`load.py`**: define la clase que maneja la exportación a Parquet.  
- **`main.py`**: archivo principal que orquesta el uso de las tres clases anteriores y ejecuta todo el flujo de trabajo.  

Ruta del archivo parquet: https://drive.google.com/file/d/1QaB-xCenb5bNCNCkl3tl0ZsuJoKFFTxN/view?usp=sharing