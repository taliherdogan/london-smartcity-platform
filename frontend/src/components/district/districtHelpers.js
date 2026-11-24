export const ZONE_LABELS = {
  commercial_hub: 'Yönetim & Ticaret Merkezi',
  financial_docklands: 'Canary Wharf & Docklands',
  dense_residential: 'Kuzey İç Şehir & Kültür',
  west_urban: 'Batı Merkez & Yerleşim',
  central_north: 'İç Kuzey & Teknoloji',
  south_central: 'Güney Merkez & Köprüler',
  transit_corridor: 'Brixton & Güney Ulaşım',
  east_logistics: 'Stratford & Olimpiyat Parkı',
  south_hub: 'Güney Ticaret & Ulaşım',
  creative_east: 'Doğu Konut & Teknoloji',
  river_port: 'Liman & Güneydoğu Geçişi',
  west_river: 'Batı Nehir & Ulaşım',
  southwest_river: 'Güneybatı Nehir & Konut',
  north_ring: 'Kuzey Çevre Yolu & Konut',
  airport_corridor: 'Batı & A40 Koridoru',
  heathrow_gateway: 'Heathrow & Batı Lojistik',
  northwest_hub: 'Wembley & Kuzeybatı',
  outer_north: 'Kuzey Sanayi & Çevre Yolu',
  far_west_aviation: 'Heathrow & Batı Sınırı',
  outer_southeast: 'Güneydoğu Yeşil & Konut',
  southeast_transit: 'Güneydoğu Konut & Koridor',
  inner_north: 'Kuzey Ulaşım & Yerleşim',
  northeast_corridor: 'Kuzeydoğu Koridoru',
  east_greenbelt: 'Doğu Orman & Konut',
  southwest_suburb: 'Wimbledon & Güneybatı',
  thames_parks: 'Thames & Kraliyet Parkları',
  southwest_market: 'Güneybatı Ticaret & Nehir',
  outer_south: 'Güney Banliyö & Konut',
  outer_northwest: 'Kuzeybatı Konut & Eğitim',
  east_industrial: 'Doğu Sanayi & Thames',
  thames_gateway: 'Thames Geçişi & Doğu',
  far_east_outer: 'En Doğu Sınır & Konut',
}

export const DISTRICT_SHORT_NAMES = {
  'Tower Hamlets': 'T. Hamlets',
  'Waltham Forest': 'W. Forest',
}

export function formatDistrictShortName(name = '') {
  return DISTRICT_SHORT_NAMES[name] || name
}

export const ENERGY_SHARE_COLORS = [
  { bg: 'bg-orange-700', dot: 'bg-orange-700' },
  { bg: 'bg-orange-600', dot: 'bg-orange-600' },
  { bg: 'bg-orange-500', dot: 'bg-orange-500' },
  { bg: 'bg-amber-600', dot: 'bg-amber-600' },
  { bg: 'bg-amber-500', dot: 'bg-amber-500' },
  { bg: 'bg-amber-400', dot: 'bg-amber-400' },
  { bg: 'bg-yellow-500', dot: 'bg-yellow-500' },
  { bg: 'bg-yellow-400', dot: 'bg-yellow-400' },
  { bg: 'bg-orange-400', dot: 'bg-orange-400' },
  { bg: 'bg-amber-300', dot: 'bg-amber-300' },
  { bg: 'bg-orange-300', dot: 'bg-orange-300' },
  { bg: 'bg-yellow-300', dot: 'bg-yellow-300' },
  { bg: 'bg-lime-500', dot: 'bg-lime-500' },
  { bg: 'bg-emerald-500', dot: 'bg-emerald-500' },
  { bg: 'bg-teal-500', dot: 'bg-teal-500' },
  { bg: 'bg-cyan-500', dot: 'bg-cyan-500' },
  { bg: 'bg-sky-500', dot: 'bg-sky-500' },
  { bg: 'bg-blue-500', dot: 'bg-blue-500' },
]

export function getTrafficLevel(speed) {
  if (speed == null) {
    return { label: '—', badge: 'bg-slate-100 text-slate-500', bar: 'bg-slate-300', pct: 0 }
  }
  if (speed < 14) {
    return {
      label: 'Çok Yoğun',
      badge: 'bg-rose-100 text-rose-700 ring-1 ring-rose-200/80',
      bar: 'bg-gradient-to-r from-rose-500 to-rose-400',
      pct: 24,
    }
  }
  if (speed < 22) {
    return {
      label: 'Yoğun',
      badge: 'bg-amber-100 text-amber-800 ring-1 ring-amber-200/80',
      bar: 'bg-gradient-to-r from-amber-500 to-amber-400',
      pct: 50,
    }
  }
  if (speed < 32) {
    return {
      label: 'Orta Akış',
      badge: 'bg-blue-100 text-blue-700 ring-1 ring-blue-200/80',
      bar: 'bg-gradient-to-r from-blue-600 to-sky-400',
      pct: 74,
    }
  }
  return {
    label: 'Akıcı',
    badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200/80',
    bar: 'bg-gradient-to-r from-emerald-500 to-teal-400',
    pct: 94,
  }
}

export function getAqiLevel(aqi) {
  if (aqi == null) {
    return { label: '—', badge: 'bg-slate-100 text-slate-500', bar: 'bg-slate-300', pct: 0 }
  }
  if (aqi <= 20) {
    return {
      label: 'İyi',
      badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200/80',
      bar: 'bg-gradient-to-r from-emerald-600 to-emerald-400',
      pct: 20,
    }
  }
  if (aqi <= 40) {
    return {
      label: 'Makul',
      badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200/80',
      bar: 'bg-gradient-to-r from-emerald-500 to-teal-400',
      pct: 40,
    }
  }
  if (aqi <= 60) {
    return {
      label: 'Orta',
      badge: 'bg-amber-100 text-amber-800 ring-1 ring-amber-200/80',
      bar: 'bg-gradient-to-r from-amber-500 to-yellow-400',
      pct: 64,
    }
  }
  if (aqi <= 80) {
    return {
      label: 'Kötü',
      badge: 'bg-orange-100 text-orange-800 ring-1 ring-orange-200/80',
      bar: 'bg-gradient-to-r from-orange-500 to-amber-400',
      pct: 84,
    }
  }
  return {
    label: 'Çok Kötü',
    badge: 'bg-rose-100 text-rose-700 ring-1 ring-rose-200/80',
    bar: 'bg-gradient-to-r from-rose-600 to-rose-400',
    pct: 100,
  }
}

export function getEnergyLevel(energy, avgEnergy) {
  if (energy == null) {
    return { label: '—', badge: 'bg-slate-100 text-slate-500', bar: 'bg-slate-300' }
  }
  if (!avgEnergy || energy >= avgEnergy * 1.12) {
    return {
      label: 'Yüksek',
      badge: 'bg-orange-100 text-orange-800 ring-1 ring-orange-200/80',
      bar: 'bg-gradient-to-r from-orange-500 to-amber-400',
    }
  }
  if (energy >= avgEnergy * 0.9) {
    return {
      label: 'Normal',
      badge: 'bg-amber-100 text-amber-800 ring-1 ring-amber-200/80',
      bar: 'bg-gradient-to-r from-amber-500 to-yellow-400',
    }
  }
  return {
    label: 'Dengeli',
    badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200/80',
    bar: 'bg-gradient-to-r from-emerald-500 to-amber-400',
  }
}

export function getTempLevel(temp) {
  if (temp == null) {
    return { label: '—', badge: 'bg-slate-100 text-slate-500', bar: 'bg-slate-300', pct: 0 }
  }
  const pct = Math.max(12, Math.min(100, Math.round(((temp + 5) / 40) * 100)))
  if (temp <= 4) {
    return {
      label: 'Soğuk',
      badge: 'bg-blue-100 text-blue-700 ring-1 ring-blue-200/80',
      bar: 'bg-gradient-to-r from-blue-600 to-sky-400',
      pct,
    }
  }
  if (temp <= 13) {
    return {
      label: 'Serin',
      badge: 'bg-cyan-100 text-cyan-800 ring-1 ring-cyan-200/80',
      bar: 'bg-gradient-to-r from-cyan-500 to-sky-400',
      pct,
    }
  }
  if (temp <= 23) {
    return {
      label: 'Ilık',
      badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200/80',
      bar: 'bg-gradient-to-r from-emerald-500 to-teal-400',
      pct,
    }
  }
  return {
    label: 'Sıcak',
    badge: 'bg-orange-100 text-orange-800 ring-1 ring-orange-200/80',
    bar: 'bg-gradient-to-r from-orange-500 to-amber-400',
    pct,
  }
}

export function getTrafficBarStyle(speed) {
  if (speed < 14) return { gradId: 'gradTrafficCritical', badge: 'bg-rose-100 text-rose-700', label: 'Çok Yoğun' }
  if (speed < 22) return { gradId: 'gradTrafficHeavy', badge: 'bg-amber-100 text-amber-800', label: 'Yoğun' }
  if (speed < 32) return { gradId: 'gradTrafficModerate', badge: 'bg-blue-100 text-blue-700', label: 'Orta Akış' }
  return { gradId: 'gradTrafficSmooth', badge: 'bg-emerald-100 text-emerald-700', label: 'Akıcı' }
}

export function getEnergyBarStyle(energy, avgEnergy) {
  if (!avgEnergy || energy >= avgEnergy * 1.12) {
    return { gradId: 'gradEnergyPeak', badge: 'bg-orange-100 text-orange-800', label: 'Yüksek Yük' }
  }
  if (energy >= avgEnergy * 0.9) {
    return { gradId: 'gradEnergyMid', badge: 'bg-amber-100 text-amber-800', label: 'Orta Yük' }
  }
  return { gradId: 'gradEnergyLow', badge: 'bg-yellow-100 text-yellow-800', label: 'Dengeli Yük' }
}

export function getAqiBarStyle(aqi) {
  if (aqi <= 20) return { gradId: 'gradAqiVeryGood', badge: 'bg-emerald-100 text-emerald-700', label: 'İyi' }
  if (aqi <= 40) return { gradId: 'gradAqiGood', badge: 'bg-emerald-50 text-emerald-700', label: 'Makul' }
  if (aqi <= 60) return { gradId: 'gradAqiModerate', badge: 'bg-amber-100 text-amber-800', label: 'Orta' }
  if (aqi <= 80) return { gradId: 'gradAqiSensitive', badge: 'bg-orange-100 text-orange-800', label: 'Kötü' }
  return { gradId: 'gradAqiBad', badge: 'bg-rose-100 text-rose-700', label: 'Çok Kötü' }
}

export function getTempBarStyle(temp) {
  if (temp <= 4) return { gradId: 'gradTempCold', badge: 'bg-blue-100 text-blue-700', label: 'Soğuk' }
  if (temp <= 13) return { gradId: 'gradTempCool', badge: 'bg-cyan-100 text-cyan-800', label: 'Serin' }
  if (temp <= 23) return { gradId: 'gradTempMild', badge: 'bg-emerald-100 text-emerald-700', label: 'Ilık' }
  return { gradId: 'gradTempWarm', badge: 'bg-orange-100 text-orange-800', label: 'Sıcak' }
}
