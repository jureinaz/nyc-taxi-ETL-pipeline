import subprocess
import logging
import time
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCRIPTS = [
    "scripts/extract.py",
    "scripts/transform.py",
    "scripts/load.py"
]

def run_script(script_path):
    logger.info(f"Ejecutando: {script_path}")
    start = time.time()
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"Error en {script_path}")
        logger.error(result.stderr)
        raise RuntimeError(f"Falló el script {script_path}")
    else:
        logger.info(f"Éxito en {script_path} ({round(time.time() - start, 2)}s)")
        logger.debug(result.stdout)

def main():
    logger.info("Iniciando pipeline ETL NYC Taxi 🚕")
    for script in SCRIPTS:
        run_script(script)
    logger.info("Pipeline ETL completado exitosamente 🎉")

if __name__ == "__main__":
    main()