###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# GÖREVLER
###############################################################

# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
# 1. flo_data_20K.csv verisini okuyunuz.
# 2. Veri setinde
# a. İlk 10 gözlem,
# b. Değişken isimleri,
# c. Betimsel istatistik,
# d. Boş değer,
# e. Değişken tipleri, incelemesi yapınız.
# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
# 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
# 5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.
# 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
# 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
# 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.

import pandas as pd
import datetime as dt

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.float_format', lambda x: '%.4f' % x)

# 1. flo_data_20K.csv verisini okuyunuz.
df_ = pd.read_csv("...")
df = df_.copy()

# 2. Veri setinde
df.head(10)
df.columns
df.describe().T
df.isnull().sum()
df.info()

# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.

df["total_price"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
df["total_order"] = df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]

# 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
date = df.columns[df.columns.str.contains("date")]
df[date] = df[date].apply(pd.to_datetime)

# 5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.
df.index  # 19945 kere alım yapılmış
df.master_id.nunique()

df.groupby("order_channel").agg({"master_id": "count",
                                 "total_order": ["sum", "mean"],
                                 "total_price": ["sum", "mean"]})

# 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
# df.groupby("master_id")["total_price"].sum().sort_values(ascending=False).head(10)
df.sort_values(by = "total_price", ascending=False).head(10)

# 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
# df.groupby("master_id")["total_order"].sum().sort_values(ascending=False).head(10)
df.sort_values(by = "total_order", ascending=False).head(10)

# 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.
def create_flo_data(dataframe):
    dataframe["total_price"] = dataframe["customer_value_total_ever_offline"] + dataframe[
        "customer_value_total_ever_online"]
    dataframe["total_order"] = dataframe["order_num_total_ever_offline"] + dataframe["order_num_total_ever_online"]
    dataframe.dropna(inplace=True)

    # df["first_order_date"] = pd.to_datetime(df["first_order_date"])
    # df["last_order_date"] = pd.to_datetime(df["last_order_date"])
    # df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
    # df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])

    date_columns ,= df.columns[df.columns.str.contains("date")]
    df[date_columns] = df[date_columns].apply(pd.to_datetime)


# GÖREV 2: RFM Metriklerinin Hesaplanması
df['last_order_date_offline'].max()
today_date = dt.datetime(2021, 6, 1)

rfm = df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                   'total_order': lambda total_order: total_order,
                                   'total_price': lambda total_price: total_price})
rfm.columns = ["recency", "frequency", "monetary"]
rfm.reset_index()

# rfm2 = pd.DataFrame()
# rfm2["customer_id"] = df["master_id"]
# rfm2["recency"] = (today_date - df["last_order_date"]).astype('timedelta64[D]')
# rfm2["frequency"] = df["total_order"]
# rfm2["monetary"] = df['total_price']


rfm.frequency.sort_values(ascending=False)
rfm.describe().T

# GÖREV 3: RF ve RFM Skorlarının Hesaplanması

rfm['recency_score'] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[5, 4, 3, 2, 1])
rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))


rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str) + rfm['monetary_score'].astype(str))

rfm.describe().T

rfm[rfm["RF_SCORE"] == '55']

# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}
rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)

# GÖREV 5: Aksiyon zamanı!
# 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
rfm.groupby("segment").agg({'recency': 'mean',
                            'frequency': 'mean',
                            'monetary': 'mean'})

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

# 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv ye kaydediniz.
# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
# tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Sadık müşterilerinden(champions,loyal_customers),
# ortalama 250 TL üzeri ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kuralacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına
# yeni_marka_hedef_müşteri_id.cvs olarak kaydediniz.
# KADIN KATEGORİSİ-ORTALAMA 250TL VE ÜZERİ ALIŞVERİŞ (champions,loyal_customers)

rfm = rfm.reset_index()

target_ids = rfm[rfm['segment'].isin(["champions","loyal_customers"])]["master_id"]
yeni_marka_hedef_müşteri_id = df[df['master_id'].isin(target_ids) & df.interested_in_categories_12.str.contains("KADIN")]["master_id"]
yeni_marka_hedef_müşteri_id.to_csv("yeni_marka_hedef_müşteri_id.csv")

# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir
# alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
# olarak kaydediniz. about_to_sleep , new_customers
target_ids = rfm[(rfm['segment'] == "new_customers") | (rfm['segment'] == "about_to_sleep")]["master_id"]
indirim_hedef_müşteri_id = df[(df['master_id'].isin(target_ids)) &
                              (df['interested_in_categories_12'].str.contains("ERKEK") |
                               df['interested_in_categories_12'].str.contains("COCUK"))]["master_id"]
target_ids.to_csv("indirim_hedef_müşteri_ids.csv")


# GÖREV 6: Tüm süreci fonksiyonlaştırınız.

def flo_rfm(dataframe, csv=False):
    create_flo_data(dataframe)

    today_date = dt.datetime(2021, 6, 1)
    df['last_order_date_offline'].max()
    rfm = df.groupby('master_id').agg(
        {'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
         'total_order': lambda total_order: total_order.nunique(),
         'total_price': lambda total_price: total_price.sum()})

    rfm.columns = ["recency", "frequency", "monetary"]

    rfm['recency_score'] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[5, 4, 3, 2, 1])
    rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

    rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))

    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }
    rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)

    if csv:
        rfm.to_csv("flo_rfm.csv")

    return rfm


df = df_.copy()
flo_rfm(df)
