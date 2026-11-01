#!/usr/bin/env bash
# =====================================================================================
#  SIMULACRO DE CORRECCIÓN - WaterManagement (SD 26/27)
#
#  Despliega WS y FO en local con Docker (cada proceso en una ventana de tmux), simula
#  las situaciones de la guía de corrección y comprueba el resultado contra la API del
#  panel de CENTRAL (Railway) y la del FO. Al final deja un informe PASS/FAIL en
#  simulacro_resultados_<fecha>.txt
#
#  Uso:
#     ./simulacro.sh                 -> todos los pasos
#     ./simulacro.sh desde <N>       -> desde el paso N
#     ./simulacro.sh solo <N>        -> solo el paso N (despliega antes si hace falta)
#     ./simulacro.sh desplegar       -> solo construye y levanta el escenario
#     ./simulacro.sh limpiar         -> para y borra todo lo del simulacro
#
#  Para ver las terminales de cada módulo (en otra terminal):
#     tmux -L sim attach -t sim      (Ctrl+b n / Ctrl+b p para cambiar de ventana,
#                                     Ctrl+b d para salir sin cerrar nada)
#
#  Configuración: variables de entorno o un fichero simulacro.env en el raíz, p. ej.:
#     CENTRAL_HTTP=https://wm-central-production.up.railway.app
#     CENTRAL=caboose.proxy.rlwy.net:56851
#     KAFKA=caboose.proxy.rlwy.net:52141
# =====================================================================================

set -u
cd "$(dirname "$0")"
[ -f simulacro.env ] && source simulacro.env

CENTRAL_HTTP="${CENTRAL_HTTP:-}"                       # URL del panel web de CENTRAL
CENTRAL="${CENTRAL:-caboose.proxy.rlwy.net:56851}"     # ip:puerto del socket de CENTRAL
KAFKA="${KAFKA:-caboose.proxy.rlwy.net:52141}"         # ip:puerto del bootstrap de Kafka

WS1="${WS1:-WS1}"; WS2="${WS2:-WS2}"; WS3="${WS3:-WS3}"
declare -A UBI=( [$WS1]="${UBI1:-Parque Lo Morant}" [$WS2]="${UBI2:-Jardin de la Universidad}" [$WS3]="${UBI3:-Parque El Palmeral}" )
declare -A PUERTO=( [OP1]=8101 [OP2]=8102 [OP3]=8103 [OP99]=8199 )

# Las carpetas pueden estar ya renombradas como pide la guía
DIR_WS=$(ls -d WM_WS ws 2>/dev/null | head -1)
DIR_FO=$(ls -d WM_FO fo 2>/dev/null | head -1)

RED=sim-net
RESULTADOS="simulacro_resultados_$(date +%Y%m%d_%H%M).txt"
NPASS=0; NFAIL=0

# ------------------------------------------------------------------------- salida
V=$'\e[32m'; R=$'\e[31m'; C=$'\e[36m'; A=$'\e[1;33m'; N=$'\e[0m'
titulo()   { echo; echo "${A}=== $* ===${N}"; echo -e "\n=== $* ===" >> "$RESULTADOS"; }
info()     { echo "${C}  > $*${N}"; }
pass()     { echo "${V}  [PASS] $*${N}"; echo "[PASS] $*" >> "$RESULTADOS"; NPASS=$((NPASS+1)); }
fail()     { echo "${R}  [FAIL] $*${N}"; echo "[FAIL] $*" >> "$RESULTADOS"; NFAIL=$((NFAIL+1)); }
check()    { local d=$1; shift; if "$@"; then pass "$d"; else fail "$d"; fi; }
pausa()    { read -rp "  ... $* (Enter para seguir) " _; }
preguntar(){ local r; read -rp "${C}  ? $* [s/n] ${N}" r
             if [[ $r =~ ^[sSyY] ]]; then pass "(visual) $*"; else fail "(visual) $*"; fi; }
esperar()  { local fin=$((SECONDS+$1)); shift
             while (( SECONDS < fin )); do "$@" && return 0; sleep 1; done; return 1; }
mayor()    { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a>b)}'; }
mayor_igual(){ awk -v a="$1" -v b="$2" 'BEGIN{exit !(a>=b)}'; }

# ------------------------------------------------------------------------- CENTRAL
api_central() { curl -fsS --max-time 5 "$CENTRAL_HTTP/api/estado" 2>/dev/null; }
central_viva(){ api_central >/dev/null; }
central_caida(){ ! central_viva; }
campo_ws()    { api_central | jq -r --arg id "$1" --arg c "$2" '.estaciones[]|select(.id==$id)|.[$c]' 2>/dev/null; }
estado_ws()   { campo_ws "$1" estado; }
es_estado()   { [ "$(estado_ws "$1")" = "$2" ]; }
no_es_estado(){ ! es_estado "$1" "$2"; }
orden()       { curl -fsS --max-time 5 -X POST -H 'Content-Type: application/json' \
                  -d "{\"accion\":\"$1\",\"ws_id\":\"$2\",\"duracion\":${3:-30}}" "$CENTRAL_HTTP/api/orden" >/dev/null; }

# ------------------------------------------------------------------------- FO
api_fo()        { curl -fsS --max-time 5 "http://localhost:${PUERTO[$1]}/api/estado" 2>/dev/null; }
fo_solicitar()  { curl -fsS --max-time 5 -X POST -H 'Content-Type: application/json' \
                    -d "{\"ws_id\":\"$2\",\"duracion\":$3}" "http://localhost:${PUERTO[$1]}/api/solicitar" | jq -r .request_id; }
fo_peticion()   { api_fo "$1" | jq -r --arg r "$2" ".peticiones[]|select(.request_id==\$r)|.${3:-estado}" 2>/dev/null; }
es_peticion()   { [ "$(fo_peticion "$1" "$2")" = "$3" ]; }
peticion_resuelta(){ local e; e=$(fo_peticion "$1" "$2"); [ -n "$e" ] && [ "$e" != ENVIADA ]; }
fo_sabe_de()    { api_fo "$1" | grep -q "$2"; }
fo_no_sabe_de() { ! fo_sabe_de "$@"; }
fo_n_estaciones(){ api_fo "$1" | jq '.estaciones|length'; }

# ------------------------------------------------------------------------- tmux / docker
T() { tmux -L sim "$@"; }
cmd_de() {   # nombre de ventana -> comando que la arranca
  local n=$1 id
  case $n in
    *-m)  id=${n%-m}; echo "docker run --rm -it --name sim-$n --network $RED wm_ws python -u WM_WS_M.py 6000 $CENTRAL $id $(printf %q "${UBI[$id]}")" ;;
    *-e)  id=${n%-e}; echo "docker run --rm -it --name sim-$n --network $RED wm_ws python -u WM_WS_E.py $KAFKA sim-$id-m:6000" ;;
    fo-*) id=${n#fo-}; echo "docker run --rm -it --name sim-$n -p ${PUERTO[$id]}:8000 wm_fo $KAFKA $id" ;;
  esac
}
lanzar() {
  local n=$1 cmd; cmd=$(cmd_de "$n")
  docker rm -f "sim-$n" >/dev/null 2>&1
  if ! T has-session -t sim 2>/dev/null; then
    T new-session -d -s sim -n info "echo 'Simulacro WaterManagement. Ctrl+b n = siguiente ventana'; bash"
    T set-window-option -g remain-on-exit on
  fi
  if T list-windows -t sim -F '#W' | grep -qx -- "$n"; then
    T respawn-window -k -t "sim:$n" "$cmd"
  else
    T new-window -d -t sim -n "$n" "$cmd"
  fi
  info "Lanzado $n"
}
matar()   { docker kill "sim-$1" >/dev/null 2>&1; info "docker kill sim-$1 (cierre súbito)"; }
ctrl_c()  { T send-keys -t "sim:$1" C-c; info "Ctrl+C en $1"; }
tecla()   { T send-keys -t "sim:$1" "$2" Enter; }
pantalla(){ T capture-pane -p -J -S - -t "sim:$1" 2>/dev/null; }
contar()  { pantalla "$1" | grep -c -- "$2"; }
hay_nuevo(){ (( $(contar "$1" "$2") > $3 )); }   # hay_nuevo ventana texto cuenta_previa
ultimas_tienen(){ pantalla "$1" | grep -v "^$" | tail -"$2" | grep -qi -- "$3"; }   # ultimas_tienen ventana n_lineas texto
construir(){ docker build -q -t "$1" "$2" >/dev/null; }

# Arranca una WS completa y espera a que el engine esté en Kafka
lanzar_ws() {
  # Al relanzar una ventana tmux borra su historial, así que basta con que aparezca una vez
  local id=$1
  lanzar "$id-m"; sleep 2; lanzar "$id-e"
  esperar 60 hay_nuevo "$id-e" "ONLINE mandado" 0 || info "AVISO: el engine de $id no ha confirmado Kafka"
}
lanzar_fo() {
  lanzar "fo-$1"
  esperar 60 hay_nuevo "fo-$1" "Conectado a Kafka" 0 || info "AVISO: el FO $1 no ha confirmado Kafka"
}

# =====================================================================================
#  PASOS
# =====================================================================================
paso_0() { titulo "0. Comprobaciones previas"
  for h in docker tmux jq curl python3; do check "Herramienta $h instalada" command -v $h; done
  if [ -z "$CENTRAL_HTTP" ]; then read -rp "  URL del panel web de CENTRAL en Railway: " CENTRAL_HTTP; fi
  CENTRAL_HTTP=${CENTRAL_HTTP%/}
  check "Panel de CENTRAL responde ($CENTRAL_HTTP)" central_viva
  check "Socket de CENTRAL accesible ($CENTRAL)" timeout 5 bash -c "</dev/tcp/${CENTRAL%:*}/${CENTRAL##*:}"
  check "Kafka accesible ($KAFKA)"               timeout 5 bash -c "</dev/tcp/${KAFKA%:*}/${KAFKA##*:}"
  info "Estructura del repositorio que pide la guía:"
  for d in WM_Central WM_WS WM_FO; do check "Existe carpeta $d" test -d $d; done
  check "Existe Nombres.txt"        test -f Nombres.txt
  check "Existe Readme.md"          bash -c "ls | grep -qix readme.md"
  check "Existen los prompts"       bash -c "ls | grep -qi '^prompts'"
  check "No hay __pycache__ subidos" bash -c "! git ls-files | grep -q __pycache__"
}

paso_1() { titulo "1. Construcción de imágenes (sin IDE)"
  check "docker build WS ($DIR_WS)" construir wm_ws "$DIR_WS"
  check "docker build FO ($DIR_FO)" construir wm_fo "$DIR_FO"
  docker network inspect $RED >/dev/null 2>&1 || docker network create $RED >/dev/null
}

paso_2() { titulo "2. Despliegue del escenario: 2 WS + 2 FO"
  info "WS registradas en la BD de CENTRAL antes de arrancar nada:"
  api_central | jq -r '.estaciones[]|"     \(.id)\t\(.ubicacion)\t\(.estado)"'
  for id in $WS1 $WS2; do
    [ -n "$(estado_ws $id)" ] && check "$id (ya registrada) aparece DESCONECTADA antes de conectar" es_estado $id DESCONECTADA
  done
  lanzar_ws $WS1; lanzar_ws $WS2
  lanzar_fo OP1; lanzar_fo OP2
  for id in $WS1 $WS2; do
    check "$id pasa a DISPONIBLE en CENTRAL"       esperar 30 es_estado $id DISPONIBLE
    check "$id aparece con su ubicación (${UBI[$id]})" test "$(campo_ws $id ubicacion)" = "${UBI[$id]}"
  done
  check "FO OP1 recibe la lista de estaciones" esperar 15 test "$(fo_n_estaciones OP1)" -gt 0
  echo
  info "Abre ahora (para seguirlo en directo):"
  info "  Panel CENTRAL : $CENTRAL_HTTP"
  info "  FO OP1 / OP2  : http://localhost:8101  http://localhost:8102"
  info "  Terminales    : tmux -L sim attach -t sim"
  pausa "Cuando lo tengas abierto"
}

paso_3() { titulo "3. Protocolo de tramas <STX>D<ETX><LRC> y autenticación"
  info "Cliente de prueba que habla directamente con el socket de CENTRAL"
  local py r d; py=$(cat <<'EOF'
import socket, sys
STX, ETX, ENQ, ACK, NACK, EOT = b'\x02', b'\x03', b'\x05', b'\x06', b'\x15', b'\x04'
def lrc(d):
    x = 0
    for b in d: x ^= b
    return x
def trama(t, mala=False):
    d = t.encode(); return STX + d + ETX + bytes([lrc(d) ^ (0xFF if mala else 0)])
def leer(s):
    buf = b''
    while True:
        i = buf.find(ETX)
        if buf.startswith(STX) and i != -1 and len(buf) >= i + 2:
            s.sendall(ACK); return buf[1:i].decode()
        c = s.recv(1024)
        if not c: raise ConnectionError
        buf += c
def res(ok, d): print(("PASS " if ok else "FAIL ") + d, flush=True)
try:
    s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=5)
    s.sendall(ENQ); res(s.recv(1) == ACK, "CENTRAL contesta ACK al ENQ")
    s.sendall(trama("AUTH#SIM_NO_EXISTE", mala=True)); res(s.recv(1) == NACK, "CENTRAL contesta NACK a una trama con LRC erróneo")
    s.sendall(trama("AUTH#SIM_NO_EXISTE")); a = s.recv(1); r = leer(s)
    res(a == ACK and r.startswith("KO_AUTH"), f"AUTH de una WS no registrada se deniega ({r})")
    s.sendall(trama("HEART#SIM_NO_EXISTE")); s.recv(1); r = leer(s)
    res(r.startswith("ERR"), f"Mensaje sin autenticar se rechaza ({r})")
    s.sendall(trama("BYE#SIM_NO_EXISTE")); s.recv(1); r = leer(s)
    res(r.startswith("OK_BYE"), "BYE + EOT cierra la sesión"); s.sendall(EOT); s.close()
except Exception as e:
    res(False, f"Error hablando con CENTRAL: {e!r}")
EOF
)
  while read -r r d; do if [ "$r" = PASS ]; then pass "$d"; else fail "$d"; fi
  done < <(python3 -c "$py" "${CENTRAL%:*}" "${CENTRAL##*:}")
  info "OJO: con el monitor real no se puede enseñar la denegación, porque siempre hace REG antes de AUTH"
}

paso_4() { titulo "4. Riego pedido desde un FO, telemetría y aislamiento entre operarios"
  local rid v1 v2; rid=$(fo_solicitar OP1 $WS1 15); info "OP1 pide $WS1 15 s -> petición $rid"
  check "FO OP1: riego autorizado (REGANDO)"           esperar 10 es_peticion OP1 "$rid" REGANDO
  check "CENTRAL: $WS1 en REGANDO"                      esperar 5 es_estado $WS1 REGANDO
  check "CENTRAL muestra el operario OP1"               test "$(campo_ws $WS1 operador)" = OP1
  check "CENTRAL muestra caudal > 0 L/min"              mayor "$(campo_ws $WS1 caudal)" 0
  v1=$(campo_ws $WS1 volumen); sleep 3; v2=$(campo_ws $WS1 volumen)
  check "El volumen acumulado crece ($v1 -> $v2 L)"     mayor "$v2" "$v1"
  check "FO OP1 recibe telemetría (volumen > 0)"        mayor "$(fo_peticion OP1 "$rid" volumen)" 0
  check "FO OP2 NO recibe mensajes del riego de OP1"    fo_no_sabe_de OP2 "$rid"
  check "FO OP2 ve todas las WS ($(fo_n_estaciones OP2))" test "$(fo_n_estaciones OP2)" = "$(api_central | jq '.estaciones|length')"
  check "FO OP1: riego FINALIZADO al acabar el tiempo"  esperar 25 es_peticion OP1 "$rid" FINALIZADA
  info "Resumen recibido: $(fo_peticion OP1 "$rid" resumen)"
  check "CENTRAL: $WS1 vuelve a DISPONIBLE"             esperar 5 es_estado $WS1 DISPONIBLE
}

paso_5() { titulo "5. Denegaciones"
  local rid ocupada
  rid=$(fo_solicitar OP1 SIM_NO_EXISTE 10)
  check "Se deniega el riego en una WS que no existe"  esperar 10 es_peticion OP1 "$rid" DENEGADA
  rid=$(fo_solicitar OP1 $WS1 5000)
  check "Se deniega una duración no válida (5000 s)"   esperar 10 es_peticion OP1 "$rid" DENEGADA
  ocupada=$(fo_solicitar OP1 $WS2 10); esperar 10 es_peticion OP1 "$ocupada" REGANDO
  rid=$(fo_solicitar OP2 $WS2 10)
  check "Se deniega pedir una WS que ya está regando"  esperar 10 es_peticion OP2 "$rid" DENEGADA
  info "Operario que no está dado de alta (OP99)"
  lanzar_fo OP99
  rid=$(fo_solicitar OP99 $WS1 10)
  check "Se deniega el riego a un operario no dado de alta" esperar 10 es_peticion OP99 "$rid" DENEGADA
  matar fo-OP99
  esperar 15 es_peticion OP1 "$ocupada" FINALIZADA
}

paso_6() { titulo "6. Riego desde el menú de la propia WS"
  tecla $WS1-e 1; sleep 1; tecla $WS1-e 20
  check "CENTRAL: $WS1 en REGANDO pedido desde la WS"  esperar 10 es_estado $WS1 REGANDO
  check "CENTRAL muestra el operario LOCAL"             test "$(campo_ws $WS1 operador)" = LOCAL
  sleep 2; tecla $WS1-e 2
  check "Parar desde el menú de la WS -> DISPONIBLE"    esperar 5 es_estado $WS1 DISPONIBLE
}

paso_7() { titulo "7. Órdenes de CENTRAL a una WS o a todas"
  local rid base
  orden START $WS1 30
  check "CENTRAL inicia un riego en $WS1"                     esperar 10 es_estado $WS1 REGANDO
  check "CENTRAL aparece como operario"                       test "$(campo_ws $WS1 operador)" = CENTRAL
  base=$(contar $WS1-e "CERRADA (BLOCKED)")
  orden BLOCK $WS1
  check "Bloquear $WS1 -> FUERA_SERVICIO"                     esperar 5 es_estado $WS1 FUERA_SERVICIO
  check "El engine de $WS1 cierra la válvula al bloquearse"   esperar 5 hay_nuevo $WS1-e "CERRADA (BLOCKED)" "$base"
  rid=$(fo_solicitar OP1 $WS1 10)
  check "Un FO no puede regar en una WS bloqueada"            esperar 10 es_peticion OP1 "$rid" DENEGADA
  orden UNBLOCK $WS1
  check "Activar $WS1 -> DISPONIBLE"                          esperar 5 es_estado $WS1 DISPONIBLE
  orden START '*' 10
  check "Iniciar riego en TODAS: $WS1 regando"                esperar 10 es_estado $WS1 REGANDO
  check "Iniciar riego en TODAS: $WS2 regando"                esperar 10 es_estado $WS2 REGANDO
  orden BLOCK '*'
  check "Bloquear TODAS: $WS1 fuera de servicio"              esperar 5 es_estado $WS1 FUERA_SERVICIO
  check "Bloquear TODAS: $WS2 fuera de servicio"              esperar 5 es_estado $WS2 FUERA_SERVICIO
  preguntar "El panel de CENTRAL muestra las WS bloqueadas en NARANJA con la leyenda 'Out of Service'"
  orden UNBLOCK '*'
  check "Activar TODAS: $WS1 disponible"                      esperar 5 es_estado $WS1 DISPONIBLE
  check "Activar TODAS: $WS2 disponible"                      esperar 5 es_estado $WS2 DISPONIBLE
}

paso_8() { titulo "8. Fuga simulada con una tecla en el engine"
  local rid base; rid=$(fo_solicitar OP1 $WS1 60); esperar 10 es_peticion OP1 "$rid" REGANDO
  base=$(contar $WS1-m "Averia, aviso")
  tecla $WS1-e k
  check "CENTRAL: $WS1 pasa a FUGA (rojo)"                     esperar 5 es_estado $WS1 FUGA
  check "El riego en curso se corta y el FO recibe el resumen" esperar 5 es_peticion OP1 "$rid" FINALIZADA
  check "El monitor muestra la avería"                         esperar 3 hay_nuevo $WS1-m "Averia, aviso" "$base"
  rid=$(fo_solicitar OP1 $WS1 10)
  check "No se puede pedir riego con fuga"                     esperar 10 es_peticion OP1 "$rid" DENEGADA
  preguntar "La WS con fuga se ve en ROJO en CENTRAL"
  tecla $WS1-e k
  check "Fuga resuelta: $WS1 vuelve a DISPONIBLE"              esperar 6 es_estado $WS1 DISPONIBLE
}

paso_9() { titulo "9. Caída de un MONITOR (WM_WS_M)"
  local rid base
  rid=$(fo_solicitar OP1 $WS1 60); esperar 10 es_peticion OP1 "$rid" REGANDO
  base=$(contar $WS1-e "VALVULA CERRADA")
  matar $WS1-m
  check "CENTRAL marca $WS1 como DESCONECTADA"               esperar 5 es_estado $WS1 DESCONECTADA
  check "El riego en curso se corta (válvula cerrada)"       esperar 8 hay_nuevo $WS1-e "VALVULA CERRADA" "$base"
  check "El FO recibe el resumen del riego"                  esperar 10 es_peticion OP1 "$rid" FINALIZADA
  tecla $WS1-e 1
  check "El engine no deja regar ni manualmente"             esperar 3 ultimas_tienen $WS1-e 5 "No se puede pedir riego"
  preguntar "El engine muestra un mensaje del tipo 'Averiada' / sin monitor"
  lanzar $WS1-m
  check "Monitor restaurado: $WS1 vuelve a DISPONIBLE"       esperar 30 es_estado $WS1 DISPONIBLE

  info "Ahora con Ctrl+C en el monitor"
  ctrl_c $WS1-m
  check "Ctrl+C en el monitor: $WS1 DESCONECTADA"            esperar 5 es_estado $WS1 DESCONECTADA
  lanzar $WS1-m;                                             esperar 30 es_estado $WS1 DISPONIBLE

  info "Monitor y engine caídos a la vez"
  matar $WS2-e; matar $WS2-m
  check "Monitor KO + Engine KO: $WS2 DESCONECTADA"          esperar 5 es_estado $WS2 DESCONECTADA
  lanzar_ws $WS2
  check "$WS2 restaurada -> DISPONIBLE"                      esperar 30 es_estado $WS2 DISPONIBLE
}

paso_10() { titulo "10. Caída de un ENGINE (WM_WS_E)"
  local rid vol
  rid=$(fo_solicitar OP1 $WS1 60); esperar 10 es_peticion OP1 "$rid" REGANDO
  sleep 5; vol=$(campo_ws $WS1 volumen); info "Volumen regado antes de la caída: $vol L"
  matar $WS1-e
  check "Monitor OK + Engine KO: $WS1 pasa a FUGA/averiada (rojo)" esperar 8 es_estado $WS1 FUGA
  check "El FO recibe el aviso / resumen del riego"            esperar 10 es_peticion OP1 "$rid" FINALIZADA
  check "No se pierde lo regado (FO: $(fo_peticion OP1 "$rid" volumen) L >= $vol L)" \
        mayor_igual "$(fo_peticion OP1 "$rid" volumen)" "$vol"
  lanzar_ws_engine_solo() { lanzar $WS1-e; esperar 60 hay_nuevo $WS1-e "ONLINE mandado" 0; }
  lanzar_ws_engine_solo
  check "Engine restaurado: $WS1 vuelve a DISPONIBLE"          esperar 15 es_estado $WS1 DISPONIBLE
  preguntar "Al volver, el engine ha enviado el estado final del riego que tenía en curso"

  info "Ahora con Ctrl+C en el engine"
  ctrl_c $WS1-e
  check "Ctrl+C en el engine: $WS1 en FUGA/averiada"           esperar 8 es_estado $WS1 FUGA
  lanzar_ws_engine_solo
  check "$WS1 vuelve a DISPONIBLE"                             esperar 15 es_estado $WS1 DISPONIBLE
}

paso_11() { titulo "11. WS nueva con monitor pero sin engine"
  lanzar $WS3-m
  check "Monitor OK sin engine: $WS3 aparece averiada (no verde)" esperar 15 es_estado $WS3 FUGA
  lanzar $WS3-e; esperar 60 hay_nuevo $WS3-e "ONLINE mandado" 0
  check "Al llegar el engine: $WS3 DISPONIBLE"                 esperar 15 es_estado $WS3 DISPONIBLE
}

paso_12() { titulo "12. Caída de un FO mientras se le está regando"
  local rid; rid=$(fo_solicitar OP1 $WS2 15); esperar 10 es_peticion OP1 "$rid" REGANDO
  matar fo-OP1
  sleep 3
  check "El riego sigue aunque el FO haya caído"            es_estado $WS2 REGANDO
  esperar 30 es_estado $WS2 DISPONIBLE
  lanzar_fo OP1
  check "Al volver, el FO ve el resultado de su servicio"   esperar 15 es_peticion OP1 "$rid" FINALIZADA
}

paso_13() { titulo "13. Caída de CENTRAL (manual en Railway)"
  local rid rid2 base
  rid=$(fo_solicitar OP1 $WS1 60); esperar 10 es_peticion OP1 "$rid" REGANDO
  info "Riego $rid en marcha en $WS1 (60 s)."
  info "AHORA: en Railway, servicio de CENTRAL -> Restart (o Settings -> pausar)."
  info "Esperando a que CENTRAL deje de responder (máx. 3 min)..."
  if ! esperar 180 central_caida; then fail "CENTRAL no ha caído, se salta el paso"; return; fi
  pass "CENTRAL caída"
  base=$(contar $WS1-e "regando...")
  check "La WS sigue regando con CENTRAL caída"              esperar 5 hay_nuevo $WS1-e "regando..." "$base"
  check "El monitor muestra que no puede conectar con CENTRAL" esperar 10 ultimas_tienen $WS1-m 20 "No puedo hablar"
  rid2=$(fo_solicitar OP2 $WS2 10); info "OP2 pide $WS2 con CENTRAL caída -> $rid2"
  info "Reiniciamos el monitor de $WS2 con CENTRAL caída"
  lanzar $WS2-m
  preguntar "El FO y el engine muestran un error tipo 'Imposible conectar con WM_Central'"
  info "Espera a que CENTRAL vuelva (máx. 5 min)..."
  check "CENTRAL vuelve a responder"                         esperar 300 central_viva
  check "Tras reiniciar, CENTRAL carga las WS de SQLite"     test "$(api_central | jq '.estaciones|length')" -gt 0
  check "$WS1 se reconecta sola"                             esperar 40 no_es_estado $WS1 DESCONECTADA
  check "El monitor de $WS2 arrancado sin CENTRAL se conecta al volver" esperar 40 es_estado $WS2 DISPONIBLE
  check "La petición hecha con CENTRAL caída recibe respuesta (no se pierde)" esperar 30 peticion_resuelta OP2 "$rid2"
  info "Esperando a que acabe el riego de $WS1..."
  check "El riego hecho durante la caída llega al FO como FINALIZADO" esperar 90 es_peticion OP1 "$rid" FINALIZADA
  es_estado $WS2 DESCONECTADA && { info "Relanzando el monitor de $WS2 para seguir"; lanzar $WS2-m; esperar 30 es_estado $WS2 DISPONIBLE; }
}

paso_14() { titulo "14. Nuevas instancias en caliente"
  T list-windows -t sim -F '#W' | grep -qx "$WS3-e" || lanzar_ws $WS3
  lanzar_fo OP3
  local rid; rid=$(fo_solicitar OP3 $WS3 8)
  check "Nuevo FO (OP3) riega en la nueva WS ($WS3)"         esperar 10 es_peticion OP3 "$rid" REGANDO
  check "El riego de OP3 termina bien"                       esperar 20 es_peticion OP3 "$rid" FINALIZADA
  check "$WS1 sigue funcionando"                             es_estado $WS1 DISPONIBLE
  check "$WS2 sigue funcionando"                             es_estado $WS2 DISPONIBLE
}

paso_15() { titulo "15. Revisión visual y acabado"
  local w trazas=""
  for w in $(T list-windows -t sim -F '#W'); do pantalla "$w" | grep -q Traceback && trazas+=" $w"; done
  check "Ninguna terminal muestra un Traceback de Python${trazas:+ (hay en:$trazas)}" test -z "$trazas"
  preguntar "CENTRAL: DISPONIBLE verde, REGANDO verde parpadeante con L/min, litros y operario, FUGA roja, DESCONECTADA gris"
  preguntar "CENTRAL muestra para cada WS la cantidad de agua suministrada (no solo durante el riego)"
  preguntar "Los logs de CENTRAL en Railway permiten seguir todo lo que ha pasado"
  preguntar "Las terminales de WS y FO se entienden sin explicaciones (sin ruido ni nombres raros)"
  preguntar "La memoria explica estos mismos pasos de despliegue"
}

resumen() {
  echo; echo "${A}=== RESUMEN: ${V}$NPASS PASS${A} / ${R}$NFAIL FAIL${A} ===${N}"
  echo -e "\n=== RESUMEN: $NPASS PASS / $NFAIL FAIL ===" >> "$RESULTADOS"
  info "Informe guardado en $RESULTADOS"
  grep '^\[FAIL\]' "$RESULTADOS" | sed 's/^/     /'
}

limpiar() {
  T kill-server 2>/dev/null
  docker ps -aq --filter name='^sim-' | xargs -r docker rm -f >/dev/null
  docker network rm $RED >/dev/null 2>&1
  echo "Simulacro limpiado"
}

# =====================================================================================
ULTIMO=15
ejecutar() { local i; for ((i=$1; i<=$2; i++)); do
  if (( i > 2 )); then read -rp "${C}>> Paso $i: Enter = ejecutar, s = saltar, q = terminar ${N}" r
    [[ $r == q ]] && break; [[ $r == s ]] && continue; fi
  paso_$i; done; }
necesita_escenario() {
  T has-session -t sim 2>/dev/null || { paso_1; paso_2; }
}

echo "Simulacro $(date)  CENTRAL_HTTP=${CENTRAL_HTTP:-?} CENTRAL=$CENTRAL KAFKA=$KAFKA" > "$RESULTADOS"
case "${1:-todo}" in
  limpiar)   limpiar; rm -f "$RESULTADOS"; exit ;;
  desplegar) paso_0; paso_1; paso_2 ;;
  solo)      N=${2:?Falta el número de paso}; paso_0; (( N > 2 )) && necesita_escenario; paso_$N ;;
  desde)     N=${2:?Falta el número de paso}; paso_0; (( N > 2 )) && necesita_escenario; ejecutar $((N > 0 ? N : 1)) $ULTIMO ;;
  todo)      ejecutar 0 $ULTIMO ;;
  *)         sed -n '2,25p' "$0"; exit 1 ;;
esac
resumen
read -rp "¿Parar y borrar el escenario del simulacro? [s/n] " r; [[ $r =~ ^[sS] ]] && limpiar
