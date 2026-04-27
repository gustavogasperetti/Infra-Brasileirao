import os

# ---------------------------------------------------------------------------
# Credenciais de acesso — Ogol
# ---------------------------------------------------------------------------
#
# Formato: lista de tuplas (email, senha)
OGOL_ACCOUNTS: list[tuple[str, str]] = [
    ("fewonak657@mugstock.com",  "Teste123!"),   # conta principal
    ("losago8838@pertok.com",    "Teste123!"),   # fallback 1  ← preencha
    ("fewonak657@mugstock.com",     "senha3"),              # fallback 2 
]

# URL de login
OGOL_LOGIN_URL: str = "https://www.ogol.com.br/login.php"

# ---------------------------------------------------------------------------
# Dicionário mapeando o Ano para a URL base daquela edição
URLS_OGOL_BRASILEIRAO = {
    1971: "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1971/2477/calendario",
    1972: "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1972/2481/calendario",
    1973: "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1973/2482/calendario",
    1974: "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1974/2483/calendario",
    1975: "https://www.ogol.com.br/edicao/copa-brasil-1975/2491/calendario",
    1976: "https://www.ogol.com.br/edicao/copa-brasil-1976/4002/calendario",
    1977: "https://www.ogol.com.br/edicao/copa-brasil-1977/3985/calendario",
    1978: "https://www.ogol.com.br/edicao/copa-brasil-1978/3903/calendario",
    1979: "https://www.ogol.com.br/edicao/brasileiro-1979/3866/calendario",
    1980: "https://www.ogol.com.br/edicao/copa-brasil-1980/3827/calendario",
    1981: "https://www.ogol.com.br/edicao/taca-de-ouro-1981/3822/calendario",
    1982: "https://www.ogol.com.br/edicao/taca-de-ouro-1982/3663/calendario",
    1983: "https://www.ogol.com.br/edicao/taca-de-ouro-1983/3643/calendario",
    1984: "https://www.ogol.com.br/edicao/copa-brasil-1984/3530/calendario",
    1985: "https://www.ogol.com.br/edicao/taca-de-ouro-1985/3512/calendario",
    1986: "https://www.ogol.com.br/edicao/copa-brasil-1986/3387/calendario",
    1987: "https://www.ogol.com.br/edicao/copa-uniao-1987/3335/calendario",
    1988: "https://www.ogol.com.br/edicao/copa-uniao-1988/2700/calendario",
    1989: "https://www.ogol.com.br/edicao/campeonato-brasileiro-1989/2676/calendario",
    1990: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1990/2674/calendario",
    1991: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1991/2651/calendario",
    1992: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1992/2650/calendario",
    1993: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1993/2649/calendario",
    1994: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1994/2643/calendario",
    1995: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1995/2639/calendario",
    1996: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1996/2509/calendario",
    1997: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1997/2508/calendario",
    1998: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1998/2507/calendario",
    1999: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-1999/2493/calendario",
    2000: "https://www.ogol.com.br/edicao/copa-joao-havelange-2000/2495/calendario",
    2001: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2001/2492/calendario",
    2002: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-2002/2490/calendario",
    2003: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-2003/2489/calendario",
    2004: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-2004/457/calendario",
    2005: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-2005/865/calendario",
    2006: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-2006/1247/calendario",
    2007: "https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-2007/1425/calendario",
    2008: "https://www.ogol.com.br/edicao/brasileirao-2008/2003/calendario",
    2009: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2009/5268/calendario",
    2010: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2010/13881/calendario",
    2011: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2011/21248/calendario",
    2012: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2012/42483/calendario",
    2013: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2013/56933/calendario",
    2014: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2014/67145/calendario",
    2015: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2015/79735/calendario",
    2016: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2016/96361/calendario",
    2017: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2017/104349/calendario",
    2018: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2018/122072/calendario",
    2019: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2019/131709/calendario",
    2020: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2020/142460/calendario",
    2021: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2021/154298/calendario",
    2022: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2022/162047/calendario",
    2023: "https://www.ogol.com.br/edicao/campeonato-brasileiro-2023/172507/calendario",
    2024: "https://www.ogol.com.br/edicao/brasileirao-serie-a-2024/184443/calendario",
    2025: "https://www.ogol.com.br/edicao/brasileirao-serie-a-2025/194851/calendario",
}