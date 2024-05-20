import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt


# Función de conexión a la base de datos
def conexionbd(sql):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="cesar",
        database="temperaturas"
    )

    mycursor = mydb.cursor()

    try:
        mycursor.execute(sql)

        # Si la consulta es un SELECT, obtén los resultados
        if sql.strip().lower().startswith('select'):
            result = mycursor.fetchall()
        else:
            result = None  # Para consultas que no devuelven resultados

        mydb.commit()

        return result
    except Exception as e:
        # Manejo de errores, por ejemplo, imprimir el error
        print(f"Error en la consulta: {e}")
        return None
    finally:
        mycursor.close()
        mydb.close()


# 1. Cargar datos en DataFrames
def cargar_datos():
    sql_paises = "SELECT * FROM paises"
    sql_temperaturas = "SELECT * FROM temperaturas"

    df_paises = pd.DataFrame(conexionbd(sql_paises), columns=['idpais', 'cca2', 'cca3', 'nombre', 'capital', 'region', 'subregion', 'miembroUE', 'latitud', 'longitud'])
    df_temperaturas = pd.DataFrame(conexionbd(sql_temperaturas), columns=['idtemperaturas', 'idpais', 'timestamp', 'temperatura', 'sensacion', 'minima', 'maxima', 'humedad', 'amanecer', 'atardecer'])

    # 2. Mostrar las primeras filas de cada DataFrame
    print("Primeras filas de paises:")
    print(df_paises.head())

    print("\nPrimeras filas de temperaturas:")
    print(df_temperaturas.head())

    return df_paises, df_temperaturas


# 3. Combinar las dos tablas utilizando 'idpais' como clave
def combinar_tablas(df_temperaturas, df_paises):
    df_combinado = pd.merge(df_temperaturas, df_paises, on='idpais')
    print("\nCombinación de tablas:")
    print(df_combinado)
    return df_combinado


# 4. Filtrar datos para mostrar solo los países de Europa
def filtrar_paises_europa(df_combinado):
    df_europa_filtro = df_combinado['region'] == 'europe'
    df_europa = df_combinado[df_europa_filtro]
    print("\nPaíses de Europa:")
    print(df_europa)

    return df_europa


# 5. Calcular la temperatura media para cada país y mostrar los resultados
def calcular_temperatura_media(df_europa):
    df_temperatura_media = df_europa.groupby(['nombre', 'latitud'])['temperatura'].mean().reset_index()
    print("\nTemperatura media por país en Europa:")
    print(df_temperatura_media)

    return df_temperatura_media


# 6. Crear un gráfico de barras para mostrar la temperatura media de los países en Europa
def grafico_temperatura_media_europa(df_temperatura_media):
    plt.figure(figsize=(12, 6))
    plt.bar(df_temperatura_media['nombre'], df_temperatura_media['temperatura'])
    plt.xlabel('Países')
    plt.ylabel('Temperatura Media')
    plt.title('Temperatura Media de los Países en Europa')
    plt.xticks(rotation=45)
    plt.show()


# 7. Encontrar el país con la temperatura media más alta y más baja
def encontrar_paises_extremos(df_temperatura_media):
    pais_mas_calido = df_temperatura_media[df_temperatura_media['temperatura'] == df_temperatura_media['temperatura'].max()]
    pais_mas_frio = df_temperatura_media[df_temperatura_media['temperatura'] == df_temperatura_media['temperatura'].min()]

    print(f"\nPaís con la temperatura media más alta: {pais_mas_calido.iloc[0]['nombre']} ({pais_mas_calido.iloc[0]['temperatura']} °C)")
    print(f"País con la temperatura media más baja: {pais_mas_frio.iloc[0]['nombre']} ({pais_mas_frio.iloc[0]['temperatura']} °C)")


# 8. Calcular el promedio de temperatura máxima y mínima en Europa
def calcular_promedio_temperaturas_europa(df_europa):
    temperatura_maxima_promedio = df_europa['maxima'].mean()
    temperatura_minima_promedio = df_europa['minima'].mean()

    print(f"\nPromedio de temperatura máxima en Europa: {temperatura_maxima_promedio} °C")
    print(f"Promedio de temperatura mínima en Europa: {temperatura_minima_promedio} °C")


# 9. Visualizar la evolución temporal de la temperatura para un país específico
def visualizar_evolucion_temporal(df_combinado, pais_seleccionado='spain'):
    df_pais_seleccionado = df_combinado[df_combinado['nombre'] == pais_seleccionado]
    df_pais_seleccionado.plot(x='timestamp', y='temperatura', kind='line', title=f'Evolución Temporal de la Temperatura en {pais_seleccionado}')
    plt.xlabel('Fecha')
    plt.ylabel('Temperatura')
    plt.show()


# 10. Crear un gráfico de dispersión entre la latitud y la temperatura media de los países
def grafico_dispersion_latitud_temperatura(df_temperatura_media):
    plt.figure(figsize=(10, 6))
    plt.scatter(df_temperatura_media['latitud'], df_temperatura_media['temperatura'])
    plt.xlabel('Latitud')
    plt.ylabel('Temperatura Media')
    plt.title('Relación entre Latitud y Temperatura Media en Europa')
    plt.show()


# 11. Agrupar datos por subregión y calcular la temperatura media para cada subregión
def calcular_temperatura_media_subregion(df_combinado):
    df_subregion_temperatura_media = df_combinado.groupby('subregion')['temperatura'].mean().reset_index()
    print("\nTemperatura media por subregión en Europa:")
    print(df_subregion_temperatura_media)

    return df_subregion_temperatura_media


# 12. Crear un gráfico de barras apiladas mostrando la temperatura media por subregión
def grafico_barras_apiladas_subregion(df_subregion_temperatura_media):
    plt.figure(figsize=(12, 6))
    plt.bar(df_subregion_temperatura_media['subregion'], df_subregion_temperatura_media['temperatura'])
    plt.xlabel('Subregión')
    plt.ylabel('Temperatura Media')
    plt.title('Temperatura Media por Subregión en Europa')
    plt.xticks(rotation=45)
    plt.show()


# 13. Guardar los DataFrames resultantes en archivos CSV separados
def guardar_a_csv(df_temperatura_media, df_subregion_temperatura_media):
    df_temperatura_media.to_csv('temperatura_media_paises_europa.csv', index=False)
    df_subregion_temperatura_media.to_csv('temperatura_media_subregiones_europa.csv', index=False)


# Llamar a todas las funciones
df_paises, df_temperaturas = cargar_datos()
df_combinado = combinar_tablas(df_temperaturas, df_paises)
df_europa = filtrar_paises_europa(df_combinado)
df_temperatura_media = calcular_temperatura_media(df_europa)
grafico_temperatura_media_europa(df_temperatura_media)
encontrar_paises_extremos(df_temperatura_media)
calcular_promedio_temperaturas_europa(df_europa)
visualizar_evolucion_temporal(df_combinado, pais_seleccionado='spain')
grafico_dispersion_latitud_temperatura(df_temperatura_media)
df_subregion_temperatura_media = calcular_temperatura_media_subregion(df_combinado)
grafico_barras_apiladas_subregion(df_subregion_temperatura_media)
guardar_a_csv(df_temperatura_media, df_subregion_temperatura_media)

