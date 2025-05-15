import pandas as pd
import numpy as np
import streamlit as st
import altair as alt
import os

# ========================
# CARGA Y PROCESADO GLOBAL
# ========================

@st.cache_data
def load_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_dir, 'recursos', 'Olist_Data')
    df = pd.read_csv(os.path.join(data_path, 'olist_customers_dataset.csv'))
    df2 = pd.read_csv(os.path.join(data_path, 'olist_orders_dataset.csv'))
    df_review = pd.read_csv(os.path.join(data_path, 'olist_order_reviews_dataset.csv'))
    df_items = pd.read_csv(os.path.join(data_path, 'olist_order_items_dataset.csv'))
    df_products = pd.read_csv(os.path.join(data_path, 'olist_products_dataset.csv'))
    df_translation = pd.read_csv(os.path.join(data_path, 'product_category_name_translation.csv'))

    df2["order_purchase_timestamp"] = pd.to_datetime(df2["order_purchase_timestamp"])
    df_final = df.merge(df2, on="customer_id")

    df_merge_products_translation = pd.merge(df_products, df_translation, on='product_category_name', how='left')
    df_products['product_category_name'] = df_merge_products_translation['product_category_name_english']

    return df, df2, df_final, df_review, df_items, df_products

# Llamada a la función
df, df2, df_final, df_review, df_items, df_products = load_data()

# ====================
# FUNCIONES DE GRAFICO
# ====================

def grafico_top_estados():
    st.subheader("Estados con más clientes en el rango de fechas seleccionado")

    min_fecha = df_final["order_purchase_timestamp"].min().date()
    max_fecha = df_final["order_purchase_timestamp"].max().date()

    fecha_inicio, fecha_fin = st.date_input("Selecciona un rango de fechas", [min_fecha, max_fecha])
    fecha_inicio = pd.to_datetime(fecha_inicio)
    fecha_fin = pd.to_datetime(fecha_fin)

    df_filtrado = df_final[(df_final["order_purchase_timestamp"] >= fecha_inicio) & (df_final["order_purchase_timestamp"] <= fecha_fin)]
    top_estados = df_filtrado["customer_state"].value_counts(ascending=False).head(5).reset_index()
    top_estados.columns = ['customer_state', 'num_clientes']

    chart = alt.Chart(top_estados).mark_bar(color='orange').encode(
        x=alt.X('num_clientes:Q', title='Número de Clientes'),
        y=alt.Y('customer_state:N', sort='-x', title='Estado'),
        tooltip=['customer_state', 'num_clientes']
    ).properties(title='Top 5 Estados con Más Clientes en el Rango Seleccionado', width=700, height=300)

    st.altair_chart(chart, use_container_width=True)

    tabla = df_filtrado.groupby(["customer_state", "customer_city"])["customer_id"].nunique().reset_index(name="num_clientes")
    tabla2 = df_final.groupby('customer_city')['order_id'].count().reset_index(name='num_pedidos_ciudad')
    total2 = tabla2['num_pedidos_ciudad'].sum()
    tabla2['numero de pedidos y %'] = tabla2['num_pedidos_ciudad'].apply(
        lambda x: f"{x} pedidos ({(x / total2 * 100):.1f}%)")

    tabla_completa = pd.merge(tabla, tabla2, on='customer_city')
    tabla_completa['ratio_pedidos_por_cliente'] = tabla_completa['num_pedidos_ciudad'] / tabla_completa['num_clientes']
    tabla_completa = tabla_completa.sort_values(by="num_clientes", ascending=False)
    tabla_completa.pop('num_pedidos_ciudad')

    st.subheader("Clientes por estado y ciudad")
    st.dataframe(tabla_completa)

    tabla = df_final.groupby(["customer_state", "customer_city"])["customer_id"].nunique().reset_index(name="num_clientes")
    top_n = st.slider('Selecciona cuántas ciudades mostrar (Top N)', min_value=3, max_value=20, value=10)
    top = tabla.sort_values(by='num_clientes', ascending=False).head(top_n)

    chart = alt.Chart(top).mark_bar(color='#B2F2BB').encode(
        x=alt.X('num_clientes:Q', title='Número de Pedidos'),
        y=alt.Y('customer_city:N', sort='-x', title='Ciudad'),
        tooltip=['customer_city', 'num_clientes']
    ).properties(title=f'Top {top_n} Ciudades con Más Pedidos', width=700, height=400)

    st.title("Distribución de Pedidos por Ciudad")
    st.altair_chart(chart, use_container_width=True)

def pedidos_retrasados():
    df_select_customer = df[['customer_id', 'customer_city', 'customer_state']]
    df_select_orders = df2[['order_id', 'customer_id', 'order_status','order_purchase_timestamp','order_approved_at','order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']]

    df_merge = pd.merge(df_select_customer, df_select_orders, on='customer_id')
    fechas = ['order_delivered_customer_date', 'order_estimated_delivery_date', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date']
    for col in fechas:
        df_merge[col] = pd.to_datetime(df_merge[col]).dt.date

    df_merge_delivered = df_merge[df_merge['order_status'] == "delivered"].copy()
    df_merge_delivered['too_late'] = df_merge_delivered['order_delivered_customer_date'] > df_merge_delivered['order_estimated_delivery_date']
    late_by_city = df_merge_delivered.groupby('customer_city')['too_late'].sum().sort_values(ascending=False)

    st.title("Pedidos retrasados por ciudad")
    top_n = st.slider("Selecciona cuántas ciudades quieres mostrar", min_value=5, max_value=30, value=10)

    late_df = late_by_city.reset_index()
    late_df.columns = ['Ciudad', 'Pedidos_Retrasados']
    top = late_df.head(top_n)

    chart = alt.Chart(top).mark_bar(color='#66c2a5').encode(
        y=alt.Y('Pedidos_Retrasados:Q', title='Pedidos Retrasados'),
        x=alt.X('Ciudad:N', sort='-y', title='Ciudad'),
        tooltip=['Ciudad', 'Pedidos_Retrasados']
    ).properties(title='Top ciudades con más pedidos retrasados', width=700, height=400)

    st.altair_chart(chart, use_container_width=True)

def reviews_por_estado():
    df_merge = pd.merge(df, df2, on='customer_id')
    df_merge = pd.merge(df_merge, df_review, on='order_id')

    df_merge['order_delivered_customer_date'] = pd.to_datetime(df_merge['order_delivered_customer_date'])
    df_merge['order_estimated_delivery_date'] = pd.to_datetime(df_merge['order_estimated_delivery_date'])
    df_merge['delay_days'] = (df_merge['order_delivered_customer_date'] - df_merge['order_estimated_delivery_date']).dt.days

    df_no_late = df_merge[df_merge['delay_days'] <= 0]
    df_merge_review_non_delayed = df_no_late[['order_id', 'customer_state', 'review_score', 'review_id']]

    review_count =  df_merge_review_non_delayed.groupby('customer_state')['review_id'].count()
    review_mean = round(df_merge_review_non_delayed.groupby('customer_state')['review_score'].mean(), 2)

    st.title("Análisis de Reviews por Estado")
    opcion = st.selectbox("Selecciona qué gráfico quieres visualizar:", ("Número de reviews por estado", "Media de puntuación por estado"))

    top_n = st.slider("Número de estados a mostrar", min_value=3, max_value=len(review_count), value=10)

    df_review_count = review_count.sort_values(ascending=False).reset_index()
    df_review_count.columns = ['customer_state', 'review_count']
    df_review_mean = review_mean.sort_values(ascending=False).reset_index()
    df_review_mean.columns = ['customer_state', 'review_mean']

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

def productos_por_categoria():
    df_merge_translation = pd.merge(df_products, df_translation, on='product_category_name', how='left')
    df_products['product_category_name'] = df_merge_translation['product_category_name_english']
    df_merge = pd.merge(df_items, df_products, on='product_id')
    df_merge = pd.merge(df_merge, df_review, on='order_id')

    df_merge['product_category_name'] = df_merge['product_category_name'].str.replace('_', ' ').str.capitalize()
    grouped = df_merge.groupby('product_category_name').agg({
        'review_score': 'mean',
        'order_id': 'count'
    }).rename(columns={'review_score': 'avg_review_score', 'order_id': 'count_orders'})

    st.title("Categorías de productos por puntuación media de reviews")
    top_n = st.slider("Selecciona cuántas categorías mostrar", min_value=5, max_value=30, value=10)
    opcion = st.selectbox("Selecciona qué gráfico quieres visualizar:", ("Mayor puntuación", "Menor puntuación", "Categorías con más pedidos"))

    if opcion == "Mayor puntuación":
        top = grouped.sort_values("avg_review_score", ascending=False).head(top_n).reset_index()
        chart = alt.Chart(top).mark_bar(color='#F08080').encode(
            x=alt.X('avg_review_score:Q', title='Puntuación media'),
            y=alt.Y('product_category_name:N', sort='-x', title='Categoría'),
            tooltip=['product_category_name', 'avg_review_score']
        ).properties(title='Top categorías por puntuación media')
        st.altair_chart(chart, use_container_width=True)

    elif opcion == "Menor puntuación":
        top = grouped.sort_values("avg_review_score", ascending=True).head(top_n).reset_index()
        chart = alt.Chart(top).mark_bar(color='#00CED1').encode(
            x=alt.X('avg_review_score:Q', title='Puntuación media'),
            y=alt.Y('product_category_name:N', sort='x', title='Categoría'),
            tooltip=['product_category_name', 'avg_review_score']
        ).properties(title='Categorías con menor puntuación media')
        st.altair_chart(chart, use_container_width=True)

    elif opcion == "Categorías con más pedidos":
        top = grouped.sort_values("count_orders", ascending=False).head(top_n).reset_index()
        chart = alt.Chart(top).mark_bar(color='#E6E6FA').encode(
            x=alt.X('count_orders:Q', title='Número de pedidos'),
            y=alt.Y('product_category_name:N', sort='-x', title='Categoría'),
            tooltip=['product_category_name', 'count_orders']
        ).properties(title='Categorías con más pedidos')
        st.altair_chart(chart, use_container_width=True)

def resumen_retrasos():
    df_select_customer = df[['customer_id', 'customer_city']]
    df_select_orders = df2[['order_id', 'customer_id', 'order_status','order_purchase_timestamp','order_approved_at','order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']]

    df_merge = pd.merge(df_select_customer, df_select_orders, on='customer_id')
    fechas = ['order_delivered_customer_date', 'order_estimated_delivery_date', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date']
    for col in fechas:
        df_merge[col] = pd.to_datetime(df_merge[col]).dt.date

    df_delivered = df_merge[df_merge['order_status'] == 'delivered'].copy()
    df_delivered['delay_days'] = (df_delivered['order_delivered_customer_date'] - df_delivered['order_estimated_delivery_date']).dt.days
    df_delivered['approved_time'] = (df_delivered['order_approved_at'] - df_delivered['order_purchase_timestamp']).dt.days
    df_delivered['preparation_time'] = (df_delivered['order_delivered_carrier_date'] - df_delivered['order_approved_at']).dt.days
    df_delivered['delivery_time'] = (df_delivered['order_delivered_customer_date'] - df_delivered['order_delivered_carrier_date']).dt.days

    df_late = df_delivered[df_delivered['delay_days'] > 0]

    razones = np.select([
        (df_late['delivery_time'] > df_late['preparation_time']) & (df_late['delivery_time'] > df_late['approved_time']),
        (df_late['preparation_time'] > df_late['delivery_time']) & (df_late['preparation_time'] > df_late['approved_time']),
        (df_late['approved_time'] > df_late['delivery_time']) & (df_late['approved_time'] > df_late['preparation_time'])
    ], ['envío', 'preparación', 'aprobación'], default='a_tiempo')

    df_late['delay_cause'] = razones

    percent = df_late.groupby('customer_city').size() / df_delivered.groupby('customer_city').size() * 100
    mean_delay = df_late.groupby('customer_city')['delay_days'].mean()
    cause = df_late.groupby('customer_city')['delay_cause'].first()

    resumen = pd.concat([
        percent.rename('Porcentaje de Retrasos'),
        mean_delay.rename('Media de Días de Retraso'),
        cause.rename('Causa Principal')
    ], axis=1).round(2).sort_values(by='Porcentaje de Retrasos', ascending=False).reset_index().rename(columns={'customer_city': 'Ciudad'})

    st.title("Resumen de Retrasos por Ciudad")
    st.dataframe(resumen)

# ======================
# INTERFAZ DE NAVEGACIÓN
# ======================

st.sidebar.title("📊 Navegación")
opciones = {
    "🌇️ Top Estados con Más Clientes": grafico_top_estados,
    "🚚 Pedidos Retrasados por Ciudad": pedidos_retrasados,
    "📟 Resumen de Retrasos por Ciudad": resumen_retrasos,
    "⭐ Análisis de Reviews por Estado": reviews_por_estado,
    "📦 Productos por Categoría": productos_por_categoria,
}

seleccion = st.sidebar.radio("Selecciona una visualización:", list(opciones.keys()))
opciones[seleccion]()