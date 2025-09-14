"""
ja_r2_processor.py: Upload du fichier ja360.pmtiles vers Cloudflare R2

Variables d'environnement requises:
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET
"""

import os
import sys
import logging
import threading
import hashlib
import time
from pathlib import Path
from dotenv import load_dotenv
import boto3
from boto3.s3.transfer import TransferConfig
from urllib.parse import urlparse
from datetime import datetime


def configurer_logger():
    """Configure le système de logging"""
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)


class ProgressionPourcentage:
    """Classe pour afficher la progression de l'upload"""
    
    def __init__(self, nom_fichier):
        self._taille = float(Path(nom_fichier).stat().st_size)
        self._vu = 0
        self._verrou = threading.Lock()
        self._heure_debut = time.time()
        
    def __call__(self, octets_transferes):
        with self._verrou:
            self._vu += octets_transferes
            pourcentage = (self._vu / self._taille) * 100
            
            temps_ecoule = time.time() - self._heure_debut
            if temps_ecoule > 0:
                vitesse_mbps = (self._vu / (1024 * 1024)) / temps_ecoule
                octets_restants = self._taille - self._vu
                eta_secondes = octets_restants / (self._vu / temps_ecoule) if self._vu > 0 else 0
                eta_str = f"{int(eta_secondes//60)}m{int(eta_secondes%60)}s" if eta_secondes > 0 else "0s"
            else:
                vitesse_mbps = 0
                eta_str = "calcul en cours..."
            
            sys.stdout.write(
                f"\r🚀 {self._vu/(1024*1024):.1f}/{self._taille/(1024*1024):.1f} MB "
                f"({pourcentage:.1f}%) - {vitesse_mbps:.1f} MB/s - ETA: {eta_str}     "
            )
            sys.stdout.flush()


def charger_env():
    """Charge les variables d'environnement depuis les fichiers .env"""
    chemins_env_possibles = [
        Path(__file__).resolve().parent / '.env',
        Path(__file__).resolve().parents[1] / '.env',
        Path(__file__).resolve().parents[2] / '.env',
    ]
    
    for chemin_env in chemins_env_possibles:
        if chemin_env.is_file():
            load_dotenv(dotenv_path=chemin_env)
            break
    else:
        load_dotenv()


def obtenir_client_et_bucket():
    """Configure et retourne le client R2 et le nom du bucket"""
    cle_acces = os.getenv('R2_ACCESS_KEY_ID') or os.getenv('Access_Key_ID')
    cle_secrete = os.getenv('R2_SECRET_ACCESS_KEY') or os.getenv('Secret_Access_Key')
    compte = os.getenv('R2_ACCOUNT_ID')
    endpoint = os.getenv('Endpoint')
    bucket = os.getenv('R2_BUCKET')
    
    if not compte and endpoint:
        host = urlparse(endpoint).hostname
        compte = host.split('.')[0] if host else None
    
    if not all([cle_acces, cle_secrete, compte, bucket]):
        logging.error("❌ Variables R2 manquantes: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET")
        sys.exit(1)
    
    url = endpoint or f"https://{compte}.r2.cloudflarestorage.com"
    
    client = boto3.client(
        's3',
        endpoint_url=url,
        aws_access_key_id=cle_acces,
        aws_secret_access_key=cle_secrete,
        region_name='auto'
    )
    
    return client, bucket


def trouver_ja360_pmtiles():
    """Recherche le fichier ja360.pmtiles dans les chemins possibles"""
    chemin_personnalise = os.getenv('JA360_PMTILES_PATH')
    if chemin_personnalise and Path(chemin_personnalise).is_file():
        return Path(chemin_personnalise)
    
    chemins_recherche = [
        Path.cwd() / "ja360.pmtiles",
        Path(__file__).resolve().parents[2] / "data" / "ja360" / "tile" / "ja360.pmtiles",
        Path(__file__).resolve().parents[1] / "data" / "ja360" / "tile" / "ja360.pmtiles",
        Path(__file__).resolve().parent / "data" / "ja360" / "tile" / "ja360.pmtiles",
        Path(__file__).resolve().parent / "ja360.pmtiles",
        Path(__file__).resolve().parents[1] / "data" / "ja360.pmtiles",
    ]
    
    for chemin in chemins_recherche:
        if chemin.is_file():
            return chemin
    
    return None


def calculer_hash_fichier(chemin_fichier):
    """Calcule le hash MD5 du fichier"""
    hash_md5 = hashlib.md5()
    with open(chemin_fichier, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def verifier_upload(client, bucket, cle_objet, taille_attendue):
    """Vérifie que l'upload s'est bien déroulé"""
    try:
        response = client.head_object(Bucket=bucket, Key=cle_objet)
        return response['ContentLength'] == taille_attendue
    except:
        return False


def main():
    """Fonction principale d'upload vers R2"""
    configurer_logger()
    charger_env()
    
    chemin_pmtiles = trouver_ja360_pmtiles()
    if not chemin_pmtiles:
        logging.error("❌ Fichier ja360.pmtiles introuvable")
        logging.error("💡 Générer d'abord le PMTiles avec: python scripts/ja/ja_tile_generator.py")
        sys.exit(1)
    
    taille_fichier = chemin_pmtiles.stat().st_size
    taille_fichier_mb = taille_fichier / (1024 * 1024)
    
    logging.info(f"📁 {chemin_pmtiles} ({taille_fichier_mb:.1f} MB)")
    
    hash_fichier = calculer_hash_fichier(chemin_pmtiles)
    
    taille_partie_mb = max(int(os.getenv('PART_SIZE_MB', '64')), 5)
    taille_partie = taille_partie_mb * 1024 * 1024
    
    config_transfert = TransferConfig(
        multipart_threshold=taille_partie,
        multipart_chunksize=taille_partie,
        max_concurrency=10,
        use_threads=True
    )
    
    cle_upload = os.getenv('UPLOAD_KEY', 'ja360.pmtiles')
    client, bucket = obtenir_client_et_bucket()
    progression = ProgressionPourcentage(str(chemin_pmtiles))
    
    heure_debut = time.time()
    
    try:
        arguments_extra = {
            'ContentType': 'application/x-protobuf',
            'Metadata': {
                'original-filename': chemin_pmtiles.name,
                'file-hash': hash_fichier,
                'upload-time': datetime.now().isoformat(),
                'file-size': str(taille_fichier),
                'data-source': 'JA360',
                'tile-type': 'raster-coverage',
                'zoom-range': '1-16'
            }
        }
        
        client.upload_file(
            Filename=str(chemin_pmtiles),
            Bucket=bucket,
            Key=cle_upload,
            Config=config_transfert,
            Callback=progression,
            ExtraArgs=arguments_extra
        )
        
        sys.stdout.write("\n")
        
        temps_upload = time.time() - heure_debut
        vitesse_moyenne = taille_fichier_mb / temps_upload if temps_upload > 0 else 0
        
        logging.info(f"✅ Upload terminé en {temps_upload:.1f}s ({vitesse_moyenne:.1f} MB/s)")
        
        if verifier_upload(client, bucket, cle_upload, taille_fichier):
            logging.info(f"✅ Fichier disponible: s3://{bucket}/{cle_upload}")
            domaine = os.getenv('R2_DOMAIN', 'tiles.streetradar.com')
            logging.info(f"🌐 URL future: https://{domaine}/tiles/ja360")
            logging.info(f"📊 Métadonnées:")
            logging.info(f"   • Source: JA360 street coverage data")
            logging.info(f"   • Type: Raster tiles (PNG)")
            logging.info(f"   • Zoom: 1-16")
            logging.info(f"   • Taille: {taille_fichier_mb:.1f} MB")
        else:
            logging.error("❌ Échec de la vérification")
            sys.exit(1)
            
    except Exception as e:
        sys.stdout.write("\n")
        logging.error(f"❌ Erreur: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
