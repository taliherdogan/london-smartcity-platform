# 7/24 Bulut Veri Toplayıcı (GitHub Actions → MSSQL) Kurulum Rehberi

Bu sistem sayesinde **bilgisayarınız gece kapalıyken bile** Londra'nın gerçek canlı trafik (**TfL Unified Road API**), hava durumu (**Open-Meteo**), hava kalitesi (**Open-Meteo AQI**) ve elektrik (**Elexon BMRS + NESO**) verileri her **15 dakikada bir** (günde 96 gerçek ölçüm) ücretsiz olarak toplanır.

Sabah bilgisayarınızı açıp Docker'ı başlattığınızda, yerel backend son **72 saat (3 gün)** içindeki eksik saatleri otomatik olarak indirip yerel **MSSQL (`SmartCityDB`)** veritabanınıza yazar.

---

## 5 Dakikalık Kurulum Adımları

### 1. GitHub'da Herkese Açık (Public) Yeni Bir Repo Oluşturun
1. [github.com/new](https://github.com/new) sayfasına gidin.
2. Repo adı verin (ör. `london-smartcity`).
3. Görünürlüğü **Public** seçin (*Public repolarda GitHub Actions tamamen ücretsiz ve sınırsızdır, ayrıca şifre/token gerekmez*).
4. **Create repository** butonuna basın.

### 2. Projeyi GitHub'a Gönderin
Proje klasöründe terminal (PowerShell) açıp şu komutları çalıştırın (`KULLANICI_ADINIZ` ve `REPO_ADINIZ` kısımlarını kendi bilginizle değiştirin):

```powershell
git add .
git commit -m "feat: add 24/7 cloud collector and MSSQL sync"
git branch -M main
git remote add origin https://github.com/KULLANICI_ADINIZ/REPO_ADINIZ.git
git push -u origin main
```

### 3. İlk Bulut Toplamasını Test Edin
1. GitHub'da reponuzun üst menüsünden **Actions** sekmesine tıklayın.
2. Sol menüden **24/7 London Smart City Cloud Collector** akışını seçin.
3. Sağ üstten **Run workflow → Run workflow** butonuna basın (~20 saniye içinde yeşil tik görünür).
4. Reponuzda **Branches** kısmında yeni oluşan **`data-archive`** dalına girin; `snapshots/YYYY/MM/YYYY-MM-DD.jsonl` dosyasının oluştuğunu göreceksiniz.
   - Bu dal ana kodunuzdan (`main`) tamamen ayrıdır, kod geçmişinizi kirletmez.
   - Bundan sonra bilgisayarınız kapalı olsa bile her 15 dakikada bir otomatik çalışır!

### 4. Yerel `.env` Dosyasını Güncelleyin
Proje kök dizinindeki `.env` dosyasını (yoksa `.env.example` dosyasını `.env` olarak kopyalayıp) açın ve şu satırı doldurun:

```env
MSSQL_AUTO_BACKFILL_ENABLED=true
MSSQL_AUTO_BACKFILL_HOURS=72
MSSQL_BACKFILL_ALLOW_TRAFFIC_MODEL=false
CLOUD_ARCHIVE_REPO=KULLANICI_ADINIZ/REPO_ADINIZ
CLOUD_ARCHIVE_BRANCH=data-archive
```

> **Not:** Eğer reponuzu *Private* yaparsanız, GitHub → Settings → Developer Settings → Personal Access Tokens (Fine-grained) bölümünden yalnızca ilgili repo için `Contents: Read-only` iznine sahip bir token oluşturup `CLOUD_ARCHIVE_TOKEN=...` satırına eklemeniz gerekir. **Public** repoda bu satır boş kalır.

### 5. Docker'ı Başlatın
```powershell
docker compose up -d --build
```
Backend açılır açılmaz `data-archive` dalındaki gece ölçümlerini otomatik indirip MSSQL'e yazar. İstediğiniz zaman arayüzdeki **Geçmiş Dönem** sekmesinden **"Eksik Saatleri MSSQL'e Kaydet"** butonuna basarak manuel senkronizasyon da yapabilirsiniz.

---

## SSMS'te Verileri Nasıl Ayırt Edersiniz?

MSSQL'de tüm tablolar ve view'lar (`dbo.vw_hourly_city_summary`, `dbo.vw_latest_district_status`) tek bir bütün olarak çalışır. Hangi satırın nereden geldiğini `batch_id` ve `data_source` sütunlarından görebilirsiniz:

| `batch_id` | Anlamı | Veri Gerçekliği |
| :--- | :--- | :--- |
| `>= 0` | Canlı Spark Streaming (PC açıkken) | %100 Gerçek Canlı |
| `-2` | 7/24 Bulut Toplayıcı (PC kapalıyken) | %100 Gerçek Canlı (15 dk aralıklarla TfL + Open-Meteo + Elexon + NESO) |
| `-1` | Geçmiş API Backfill (Bulut öncesi eski günler) | Hava, AQI ve Enerji %100 Gerçek Geçmiş API (Trafik modeli varsayılan kapalı) |

### Örnek SSMS Sorguları

```sql
-- 1. Gece saatleri dahil saatlik Londra özeti
SELECT TOP 72 *
FROM dbo.vw_hourly_city_summary
ORDER BY saat_londra DESC;

-- 2. Kaynağa göre MSSQL kayıt dağılımı
SELECT
    CASE
        WHEN batch_id >= 0 THEN N'Canlı Spark Streaming'
        WHEN batch_id = -2 THEN N'7/24 Bulut Toplayıcı (Gerçek TfL/API)'
        WHEN batch_id = -1 THEN N'Geçmiş API Backfill'
    END AS kaynak_tipi,
    COUNT(*) AS satir_sayisi,
    MIN(london_time) AS ilk_kayit,
    MAX(london_time) AS son_kayit
FROM dbo.domain_snapshots
GROUP BY CASE
    WHEN batch_id >= 0 THEN N'Canlı Spark Streaming'
    WHEN batch_id = -2 THEN N'7/24 Bulut Toplayıcı (Gerçek TfL/API)'
    WHEN batch_id = -1 THEN N'Geçmiş API Backfill'
END;

-- 3. Gece 00:00 - 07:00 arası buluttan yakalanan gerçek TfL trafik ölçümleri
SELECT TOP 50 london_time, avg_value AS hiz_kms, min_value, max_value, data_source
FROM dbo.domain_snapshots
WHERE domain = N'traffic'
  AND batch_id = -2
  AND DATEPART(HOUR, london_time) BETWEEN 0 AND 6
ORDER BY london_time DESC;
```
