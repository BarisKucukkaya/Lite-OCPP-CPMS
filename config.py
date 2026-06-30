"""
CPMS yapılandırması — sunucu tarafı, frontend'e gönderilmez.
Dağıtmadan önce şifreyi değiştirin.
"""

# Admin kullanıcıları: {kullanici_adi: sifre}
ADMIN_USERS = {
    "admin": "vestel2024",
}

# Session token ömrü (saniye) — varsayılan 8 saat
SESSION_TTL = 8 * 3600
