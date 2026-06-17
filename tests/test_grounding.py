"""Audit ALTO #6: grounding de gestores — un gestor del análisis que no aparece en
ninguna fuente cruda es probable invención. Conservador (nombre de pila + apellido).
"""
from tools.grounding import _name_grounded, _is_person_name, _strip


def test_person_filter_skips_generic():
    assert _is_person_name("Iván Martín Aranguez") is True
    assert _is_person_name("Equipo de High Yield de Insight Investment") is False
    assert _is_person_name("Gestores del fondo") is False


def test_grounded_when_name_in_text():
    raw = _strip("el fondo lo gestiona ivan martin aranguez desde 2015")
    assert _name_grounded("Iván Martín Aranguez", raw) is True


def test_ungrounded_when_absent():
    raw = _strip("texto que habla de otras cosas sin el gestor")
    assert _name_grounded("Charlotte Yonge", raw) is False


def test_accent_insensitive():
    raw = _strip("informe de emerito quintana sobre el fondo")
    assert _name_grounded("Emérito Quintana Pelayo", raw) is True


def test_partial_not_enough():
    # solo el nombre de pila presente (genérico) no basta: falta el apellido
    raw = _strip("jose hablaba del mercado")  # 'jose' sí, 'lecube' no
    assert _name_grounded("José María Lecube", raw) is False
