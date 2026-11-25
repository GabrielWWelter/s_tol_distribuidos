import ipaddress


def ip_para_id(ip):
    return int(ipaddress.IPv4Address(ip))


def id_para_ip(no_id):
    return str(ipaddress.IPv4Address(no_id))


def sucessor(ids, meu_id, inativos=()):
    anel = sorted(ids)
    posicao = anel.index(meu_id)
    for passo in range(1, len(anel)):
        candidato = anel[(posicao + passo) % len(anel)]
        if candidato not in inativos:
            return candidato
    return None


def tratar_eleicao(meu_id, highest_id, inativos=(), participante=False):
    if highest_id in inativos or highest_id < meu_id:
        # participante não substitui de novo: descarta a eleição concorrente
        return ("descartar" if participante else "repassar"), meu_id
    if highest_id == meu_id:
        return "eleito", meu_id
    return "repassar", highest_id
