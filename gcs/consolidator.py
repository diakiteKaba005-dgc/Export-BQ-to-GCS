from google.cloud import storage
import os

class GCSConsolidator:
    def __init__(self):
        self.client = storage.Client()

    def consolidate_shards(self, bucket_name: str, file_prefix: str, table_id: str, end_time_str: str, format_type: str) -> str:
        """
        Regroupe les éclats (shards) du sharding natif BigQuery en un fichier unique.
        Gère la limite native de GCS de 32 composants maximum par opération de composition.
        """
        bucket = self.client.bucket(bucket_name)
        
        # 1. Détermination de l'extension de fichier appropriée
        extension_map = {
            "CSV": ".csv",
            "PARQUET": ".parquet",
            "AVRO": ".avro"
        }
        ext = extension_map.get(format_type.upper(), ".csv")
        
        # Racine commune des fichiers éclats générés par le joker BigQuery
        shard_pattern = f"{file_prefix}{table_id}_{end_time_str}_"
        blobs = list(bucket.list_blobs(prefix=shard_pattern))
        
        if not blobs:
            print(f"[GCSConsolidator] Aucun éclat trouvé avec le préfixe : {shard_pattern}")
            return f"gs://{bucket_name}/{file_prefix}{table_id}_{end_time_str}{ext}"
            
        final_blob_name = f"{file_prefix}{table_id}_{end_time_str}_all{ext}"
        final_blob = bucket.blob(final_blob_name)
        
        # Cas 1 : Un seul fichier éclat généré par BigQuery -> Simple renommage propre
        if len(blobs) == 1:
            print(f"[GCSConsolidator] Un seul éclat détecté. Renommage en : {final_blob_name}")
            bucket.rename_blob(blobs[0], final_blob_name)
            return f"gs://{bucket_name}/{final_blob_name}"
            
        # Cas 2 : Plusieurs éclats -> Logique de composition par paquets de 32 maximum
        print(f"[GCSConsolidator] {len(blobs)} éclats détectés. Début de la consolidation séquentielle...")
        
        chunks = [blobs[i:i + 32] for i in range(0, len(blobs), 32)]
        temp_blobs_to_clean = []
        
        for idx, chunk in enumerate(chunks):
            # S'il s'agit du dernier paquet et qu'on a déjà fusionné des paquets précédents
            if idx == len(chunks) - 1 and idx > 0:
                final_blob.compose([final_blob] + chunk)
            # Pour le tout premier paquet de 32
            elif idx == 0:
                final_blob.compose(chunk)
            # Pour les paquets intermédiaires (on crée un fichier temporaire)
            else:
                temp_blob_name = f"{shard_pattern}temp_composite_{idx}{ext}"
                temp_blob = bucket.blob(temp_blob_name)
                temp_blob.compose(chunk)
                temp_blobs_to_clean.append(temp_blob)
                final_blob.compose([final_blob, temp_blob])

        # 3. Nettoyage des fichiers temporaires et des éclats d'origine pour libérer l'espace
        print("[GCSConsolidator] Nettoyage des éclats temporaires sur GCS...")
        for blob in blobs + temp_blobs_to_clean:
            try:
                blob.delete()
            except Exception as e:
                print(f"[WARNING] Impossible de supprimer le fichier temporaire {blob.name} : {str(e)}")
                
        return f"gs://{bucket_name}/{final_blob_name}"