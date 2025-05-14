import pandas as pd
import numpy as np
import streamlit as st
import altair as alt


#Leemos los dos archivos CSV para los dos primeros ejercicios

df = pd.read_csv('recursos/Olist_Data/olist_customers_dataset.csv') 
df2 = pd.read_csv('recursos/Olist_Data/olist_orders_dataset.csv')

#Formateamos la fecha del dataset para poder trabajar con el campo 

df2["order_purchase_timestamp"] = pd.to_datetime(df2["order_purchase_timestamp"])

#Realizamos el merge de los dos CSV utilizando el campo en común

df_final = df.merge(df2, on="customer_id")

#Obtenemos la fecha minima del dataset y máxima del dataset para introducirla como preestablecida en el dateinput de streamlit

min_fecha = df_final["order_purchase_timestamp"].min().date()
max_fecha = df_final["order_purchase_timestamp"].max().date()

#Creamos el date input, lo utilizaremos para poder variar la fecha y que el gráfico cambie en función de las fechas

fecha_inicio, fecha_fin = st.date_input("Selecciona un rango de fechas", [min_fecha, max_fecha])
   
fecha_inicio = pd.to_datetime(fecha_inicio)
fecha_fin = pd.to_datetime(fecha_fin)

df_filtrado = df_final[(df_final["order_purchase_timestamp"] >= fecha_inicio) & (df_final["order_purchase_timestamp"] <= fecha_fin)]
  
top_estados = df_filtrado["customer_state"].value_counts(ascending=False).head(5)

st.subheader("Estados con más clientes en el rango de fechas seleccionado")

#Gráfico streamlit con los 5 estados con mas clientes en un rango de fechas variable

top_estados_df = top_estados.reset_index()
top_estados_df.columns = ['customer_state', 'num_clientes']

# Gráfico con Altair ordenado de mayor a menor
chart_estados = alt.Chart(top_estados_df).mark_bar(color='orange').encode(
    x=alt.X('num_clientes:Q', title='Número de Clientes'),
    y=alt.Y('customer_state:N', sort='-x', title='Estado'),
    tooltip=['customer_state', 'num_clientes']
).properties(
    title='Top 5 Estados con Más Clientes en el Rango Seleccionado',
    width=700,
    height=300
)

st.altair_chart(chart_estados, use_container_width=True)

#================================================

tabla = df_filtrado.groupby(["customer_state", "customer_city"])["customer_id"].nunique().reset_index(name="num_clientes")
tabla2 = df_final.groupby('customer_city')['order_id'].count().reset_index(name='num_pedidos_ciudad')
total2 = tabla2['num_pedidos_ciudad'].sum()
tabla2['numero de pedidos y %'] = tabla2['num_pedidos_ciudad'].apply(
    lambda x: f"{x} pedidos ({(x / total2 * 100):.1f}%)"
)
tabla_completa = pd.merge(tabla, tabla2, on='customer_city')
tabla_completa['ratio_pedidos_por_cliente'] = tabla_completa['num_pedidos_ciudad'] / tabla_completa['num_clientes']
tabla_completa = tabla_completa.sort_values(by="num_clientes", ascending=False)
tabla_completa.pop('num_pedidos_ciudad')
st.subheader("Clientes por estado y ciudad")
st.dataframe(tabla_completa)


#========================================================================================

st.title("Distribución de Pedidos por Ciudad")

# Ordenar por cantidad de pedidos y seleccionar las top N
top_n = st.slider('Selecciona cuántas ciudades mostrar (Top N)', min_value=3, max_value=20, value=10)

# Ordenar y seleccionar top N

tabla_top = tabla_completa.sort_values(by='num_clientes', ascending=False).head(top_n)

# Crear gráfico con Altair
chart = alt.Chart(tabla_top).mark_bar(color='#B2F2BB').encode(
        x=alt.X('num_clientes:Q', title='Número de Clientes'),
        y=alt.Y('customer_city:N', sort='-x', title='Ciudad'),
        tooltip=['customer_city', 'num_clientes']
    ).properties(
        title=f'Top {top_n} Ciudades con Más Clientes',
        width=700,
        height=400
    )

st.altair_chart(chart, use_container_width=True)



#Carga de csv
df_customer = pd.read_csv('recursos\Olist_Data\olist_customers_dataset.csv')
df_orders = pd.read_csv('recursos\Olist_Data\olist_orders_dataset.csv')
df_review = pd.read_csv('recursos\Olist_Data\olist_order_reviews_dataset.csv')
df_items = pd.read_csv('recursos\Olist_Data\olist_order_items_dataset.csv')
df_products = pd.read_csv('recursos\Olist_Data\olist_products_dataset.csv')
df_translation = pd.read_csv('recursos\Olist_Data\product_category_name_translation.csv')
# Hacer merge con la tabla principal
df_merge_products_translation = pd.merge(df_products, df_translation, on='product_category_name', how='left')

# Traduccion de columna
df_products['product_category_name'] = df_merge_products_translation['product_category_name_english']


#Seleccion columnas
df_select_customer = df_customer[['customer_id', 'customer_city', 'customer_state']]
df_select_orders = df_orders[['order_id', 'customer_id', 'order_status','order_purchase_timestamp','order_approved_at','order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']]

#Merge de df
df_merge = df_merge = pd.merge(df_select_customer, df_select_orders, on='customer_id')
print(df_merge.columns)
#Dejamos la fecha solo en dias
df_merge['order_delivered_customer_date'] = pd.to_datetime(df_merge['order_delivered_customer_date']).dt.date
df_merge['order_estimated_delivery_date'] = pd.to_datetime(df_merge['order_estimated_delivery_date']).dt.date
df_merge['order_purchase_timestamp'] = pd.to_datetime(df_merge['order_purchase_timestamp']).dt.date
df_merge['order_approved_at'] = pd.to_datetime(df_merge['order_approved_at']).dt.date
df_merge['order_delivered_carrier_date'] = pd.to_datetime(df_merge['order_delivered_carrier_date']).dt.date

#Filtra los pedidos que tienen el estado entregado
delivered = df_merge['order_status'] == "delivered"
df_merge_delivered = df_merge[delivered].copy()
#Posibles estados del pedido
status = df_merge['order_status'].unique()
#Crea una columna donde True indica que el pedido llego tarde y False que llego a tiempo
df_merge_delivered['too_late'] = df_merge_delivered['order_delivered_customer_date'] > df_merge_delivered['order_estimated_delivery_date']
#Agrupa por ciudad y cuenta la cantidad de pedidos retrasados
late_by_city = df_merge_delivered.groupby('customer_city')['too_late'].sum().sort_values(ascending=False)

# Título
st.title("Pedidos retrasados por ciudad")
# Selector: número de ciudades a mostrar
top_n = st.slider("Selecciona cuántas ciudades quieres mostrar", min_value=5, max_value=30, value=10)

# Mostrar gráfica con top N ciudades
late_df = late_by_city.sort_values(ascending=False).reset_index()
late_df.columns = ['Ciudad', 'Pedidos_Retrasados']
top = late_df.head(top_n)
# Gráfico con Altair
chart = alt.Chart(top).mark_bar(color='#66c2a5').encode(
    y=alt.Y('Pedidos_Retrasados:Q', title='Pedidos Retrasados'),
    x=alt.X('Ciudad:N', sort='-y', title='Ciudad'),
    tooltip=['Ciudad', 'Pedidos_Retrasados']
).properties(
    title='Top ciudades con más pedidos retrasados',
    width=700,
    height=400
)

# Mostrar gráfico
st.altair_chart(chart, use_container_width=True)

#Agrupa por ciudad y cuenta la cantidad total de pedidos
orders_by_city = df_merge.groupby('customer_city')['order_id'].count()
#Porcentaje de retrasdos por ciudad
procent_by_city = (late_by_city/orders_by_city) * 100
#Rellenamos los posibles nulos
procent_by_city = procent_by_city.fillna(0)

#Vemos los dias retrasados
df_merge_delivered['delay_days'] = (df_merge_delivered['order_delivered_customer_date'] - df_merge_delivered['order_estimated_delivery_date']).dt.days


#Motivo del retraso
df_merge_delivered['approved_time'] = (df_merge_delivered['order_approved_at'] - df_merge_delivered['order_purchase_timestamp']).dt.days
df_merge_delivered['preparation_time'] = (df_merge_delivered['order_delivered_carrier_date'] - df_merge['order_approved_at']).dt.days
df_merge_delivered['delivery_time'] = (df_merge['order_delivered_customer_date'] - df_merge['order_delivered_carrier_date']).dt.days

# Condición: solo si hubo retraso
condiciones = [
    df_merge_delivered['delay_days'] > 0, 
]

# Evaluamos cuál de los tiempos fue mayor
razones = np.select([
        (df_merge_delivered['delivery_time'] > df_merge_delivered['preparation_time']) & 
        (df_merge_delivered['delivery_time'] > df_merge_delivered['approved_time']),
        
        (df_merge_delivered['preparation_time'] > df_merge_delivered['delivery_time']) & 
        (df_merge_delivered['preparation_time'] > df_merge_delivered['approved_time']),
        
        (df_merge_delivered['approved_time'] > df_merge_delivered['delivery_time']) & 
        (df_merge_delivered['approved_time'] > df_merge_delivered['preparation_time']),],
    ['envío', 'preparación', 'aprobación'],
    default='a_tiempo'
)
# Asignamos al dataframe
df_merge_delivered['delay_cause'] = razones
#Filtramos solo los retrasos (días > 0)
late_only = df_merge_delivered[df_merge_delivered['delay_days'] > 0]
#Media de retrasos por ciudad
mean_delay_by_city = late_only.groupby('customer_city')['delay_days'].mean().sort_values(ascending=False)


# Solo ciudades con retraso
ciudades_con_retraso = late_only['customer_city'].unique()

# Filtramos el porcentaje solo a esas ciudades
procent_by_city_filtrado = procent_by_city[procent_by_city.index.isin(ciudades_con_retraso)]

#Titulo
st.title("Resumen de Retrasos por Ciudad")
causa_principal = late_only.groupby('customer_city')['delay_cause'].first()
causa_principal = causa_principal.rename("Causa Principal")
 
# Agrupamos por ciudad 
resumen = pd.concat([
        procent_by_city_filtrado.rename("Porcentaje de Retrasos"),
        mean_delay_by_city.rename("Media de Días de Retraso"),
        causa_principal
          ], axis=1).round(2).sort_values(by="Porcentaje de Retrasos", ascending=False)
# Cambiamos nombre de primera columna
resumen = resumen.reset_index().rename(columns={'customer_city': 'Ciudad'})
st.dataframe(resumen)



#Filtramos los pedidos sin delay
df_no_late = df_merge_delivered[df_merge_delivered['delay_days'] <= 0]
#Hacemos merge de tabla de pedidos y de reviews
df_merge_review_non_delayed = pd.merge(df_no_late, df_review,on = 'order_id')
#Selecciona columnas a utilizar
df_merge_review_non_delayed = df_merge_review_non_delayed[['order_id', 'customer_state', 'review_score', 'review_id']]
#Cuenta las reviews por estado
review_count =  df_merge_review_non_delayed.groupby('customer_state')['review_id'].count()
#Hace la media de las notas de esas reviews
review_mean = round(df_merge_review_non_delayed.groupby('customer_state')['review_score'].mean(),2)
#Agrupamos columnas
review_stats = pd.concat([review_count, review_mean], axis=1)





# Título
st.title("Análisis de Reviews por Estado")
# Selector 1: tipo de gráfico
opcion = st.selectbox("Selecciona qué gráfico quieres visualizar:",
    ("Número de reviews por estado", "Media de puntuación por estado"))

# Selector 2: cuántos estados mostrar 
# Mira cuantos estados hay en total
max_estados = len(review_count)
# Deja seleccionar el numero, con un minimo, un maximo y estableciendo el estandar en 10
top_n = st.slider("Número de estados a mostrar", min_value=3, max_value=max_estados, value=10)
#Convierte las series a Df
df_review_count = review_count.sort_values(ascending=False).reset_index()
df_review_count.columns = ['customer_state', 'review_count']
df_review_mean = review_mean.sort_values(ascending=False).reset_index()
df_review_mean.columns = ['customer_state', 'review_mean']

# Lógica según la opción seleccionada
if opcion == "Número de reviews por estado":
    top = df_review_count.head(top_n)
    chart = alt.Chart(top).mark_bar(color='#6495ED').encode(
        x=alt.X('review_count:Q', title='Número de reviews'),
        y=alt.Y('customer_state:N', sort='-x', title='Estado'),
        tooltip=['customer_state', 'review_count']
    ).properties(title='Top estados por número de reviews')
    st.altair_chart(chart, use_container_width=True)

elif opcion == "Media de puntuación por estado":
    top = df_review_mean.head(top_n)
    chart = alt.Chart(top).mark_bar(color='#20B2AA').encode(
        x=alt.X('review_mean:Q', title='Puntuación media'),
        y=alt.Y('customer_state:N', sort='-x', title='Estado'),
        tooltip=['customer_state', 'review_mean']
    ).properties(title='Top estados por puntuación media')
    st.altair_chart(chart, use_container_width=True)





#Merge de tablas de productos y lista de items por pedido
df_merge_items_products = pd.merge(df_items, df_products, on='product_id')
# Selección de columnas
df_merge_items_products = df_merge_items_products[['order_id', 'product_id', 'product_category_name']]
#Merge de items y reviews
df_merge_products_orders = pd.merge(df_merge_items_products,  df_review, on= 'order_id')
#Selección de columnas
df_merge_products_reviews = df_merge_products_orders[['order_id', 'product_id', 'product_category_name', 'review_score']]

#Añadir columna que cuente la cantidad de pedidos por categoría de producto, con transform respecta el número de filas
df_merge_products_reviews['count_orders_by_category'] = df_merge_products_reviews.groupby('product_category_name')['order_id'].transform('count')


#Se mejora el texto para presentación en gráfica
df_merge_products_reviews['product_category_name'] = df_merge_products_reviews['product_category_name'].str.replace('_', ' ').str.capitalize()

# Agrupamiento por categoría
grouped_by_category = df_merge_products_reviews.groupby('product_category_name').agg({
    'review_score': 'mean',
    'order_id': 'count'  
}).rename(columns={'review_score': 'avg_review_score', 'order_id': 'count_orders'})




# Título
st.title("Categorías de productos por puntuación media de reviews")
# Selector: número de ciudades a mostrar
top_n = st.slider("Selecciona cuántas categorías mostrar", min_value=5, max_value=30, value=10)

top = grouped_by_category.sort_values("avg_review_score", ascending=False).head(top_n).reset_index()

# Selector 1: tipo de gráfico
opcion = st.selectbox("Selecciona qué gráfico quieres visualizar:",
    ("Mayor puntuación", "Menor puntuación", "Categorías con más pedidos"))

# Mostrar gráfico según opción
if opcion == "Mayor puntuación":
    top = grouped_by_category.sort_values("avg_review_score", ascending=False).head(top_n).reset_index()
    chart = alt.Chart(top).mark_bar(color='#F08080').encode(
        x=alt.X('avg_review_score:Q', title='Puntuación media'),
        y=alt.Y('product_category_name:N', sort='-x', title='Categoría'),
        tooltip=['product_category_name', 'avg_review_score']
    ).properties(title='Top categorías por puntuación media')
    st.altair_chart(chart, use_container_width=True)

elif opcion == "Menor puntuación":
    top = grouped_by_category.sort_values("avg_review_score", ascending=True).head(top_n).reset_index()
    chart = alt.Chart(top).mark_bar(color='#00CED1').encode(
        x=alt.X('avg_review_score:Q', title='Puntuación media'),
        y=alt.Y('product_category_name:N', sort='x', title='Categoría'),
        tooltip=['product_category_name', 'avg_review_score']
    ).properties(title='Categorías con menor puntuación media')
    st.altair_chart(chart, use_container_width=True)

elif opcion == "Categorías con más pedidos":
    top = grouped_by_category.sort_values("count_orders", ascending=False).head(top_n).reset_index()
    chart = alt.Chart(top).mark_bar(color='#E6E6FA').encode(
        x=alt.X('count_orders:Q', title='Número de pedidos'),
        y=alt.Y('product_category_name:N', sort='-x', title='Categoría'),
        tooltip=['product_category_name', 'count_orders']
    ).properties(title='Categorías con más pedidos')
    st.altair_chart(chart, use_container_width=True)
