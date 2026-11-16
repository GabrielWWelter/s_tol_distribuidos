import ipaddress


def ip_para_id(ip):
    return int(ipaddress.IPv4Address(ip))


def sucessor(ids, meu_id, inativos=()):
    anel = sorted(ids)
    posicao = anel.index(meu_id)
    for passo in range(1, len(anel)):
        candidato = anel[(posicao + passo) % len(anel)]
        if candidato not in inativos:
            return candidato
    return None


def tratar_eleicao(meu_id, highest_id, participante):
    if highest_id == meu_id:
        return "eleito", meu_id
    if highest_id > meu_id:
        return "repassar", highest_id
    if participante:
        return "descartar", None
    return "repassar", meu_id
