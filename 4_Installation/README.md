# Flash Sale — Environnement d’Installation

Ce dossier permet de **déployer Redis** pour le projet *Flash Sale — Gestion de Stock (NoSQL Redis)*.

L’objectif est d’avoir un environnement **reproductible, portable et prêt pour la démonstration**.

---

## 📁 Contenu du dossier

| Fichier | Rôle |
|---------|------|
| `docker-compose.yml` | Lance Redis 7 avec persistance et healthcheck |
| `setup_instructions.txt` | Guide détaillé d’installation étape par étape |
| `README.md` | Vue rapide pour démarrage immédiat |

---

## 🎯 Objectif

Fournir un environnement Redis :

- prêt à l’emploi
- sécurisé via ACL (username + password)
- persistant (données conservées)
- surveillé (healthcheck Docker)

Ce dossier transforme le projet en solution **déployable**.

---

## 🚀 Démarrage rapide

### 1️⃣ Lancer Redis

Depuis ce dossier :

```powershell
docker compose up -d
2️⃣ Vérifier que Redis fonctionne
docker ps
Puis :

docker inspect --format="{{.State.Health.Status}}" redis-flash
Résultat attendu :

healthy
3️⃣ Créer l’utilisateur sécurisé (ACL Redis)
docker exec -it redis-flash redis-cli
ACL SETUSER khalid on >5002 allcommands allkeys
ACL LIST
exit
4️⃣ Définir les variables d’environnement (PowerShell)
Dans la racine du projet FlashSale :

$env:REDIS_USERNAME="khalid"
$env:REDIS_PASSWORD="5002"
5️⃣ Tester la connexion
python app.py ping
Résultat attendu :

PONG
🧠 Pourquoi cette installation est professionnelle
Élément	Justification
Docker Compose	Reproductibilité
Volume Redis	Persistance des données
AppendOnly (AOF)	Sécurité des écritures
Healthcheck	Surveillance du service
ACL Redis	Sécurité d’accès
🛑 Arrêt
docker compose down
Suppression complète des données :

docker compose down -v
🧩 Remarque importante
Les identifiants Redis (username/password) sont injectés via variables d’environnement.
Cela permet de ne pas exposer les secrets dans le code.
