# Proje Çalışma Kuralları

## Docker Servislerini Otomatik Güncelleme
- Bu proje Docker Compose (`docker-compose.yml`) üzerinden çalışır; yalnızca yerel dosyaları düzenlemek veya yerel `npm run build` çalıştırmak kullanıcının ekranda gördüğü çalışan uygulamayı güncellemez.
- Kod veya yapılandırma dosyalarında yapılan her değişiklikten sonra, etkilenen Docker servis(ler)ini mutlaka yeniden derleyip başlat:
  - `frontend/` değişikliklerinde: `docker compose up -d --build frontend`
  - `services/backend/` değişikliklerinde: `docker compose up -d --build backend`
  - `services/generator/` değişikliklerinde: `docker compose up -d --build generator`
  - `services/spark_processor/` değişikliklerinde: `docker compose up -d --build spark-processor`
  - `.env` veya `docker-compose.yml` değişikliklerinde: etkilenen servisleri `docker compose up -d` ile yeniden oluştur.
- Değişiklik tamamlandı bilgisini yalnızca ilgili Docker konteyneri yeniden başlatılıp sağlıklı duruma geçtikten sonra ver.
