# Terraform, Infrastruktur in der DHBW Cloud

Provisioniert das k3s-Cluster für die MES-Pipeline: ein Master und zwei Worker in der
DHBW-Cloud (`newstack.dhbw.cloud`), plus das Ansible-Inventar für den nächsten Schritt.

## Aufbau

Die übliche Aufteilung: [`versions.tf`](versions.tf) (Provider `openstack` und `local`),
[`variables.tf`](variables.tf) (Projekt, Image-Name, Flavors, Key-Pair, Netz, Provider-Block),
[`main.tf`](main.tf) (die drei Instanzen und das generierte Inventar) und
[`outputs.tf`](outputs.tf) (IPv6-Adressen, Inventar-Pfad). `flavors.txt` ist die abgelegte
Ausgabe von `openstack flavor list`, damit die Flavor-Wahl unten nachvollziehbar bleibt.

| Ressource | Was |
|---|---|
| `data.openstack_images_image_v2` | löst das Image über den Namen auf, nicht über eine feste ID |
| `openstack_compute_instance_v2.master` | `general.medium`, 4 vCPU / 16 GB / 80 GB |
| `openstack_compute_instance_v2.worker` (×2) | `general.small`, 2 vCPU / 8 GB / 50 GB je Knoten |
| `local_file.ansible_inventory` | schreibt `generated-inventory.yml` aus den echten Adressen |

**Macht 8 vCPU insgesamt** (4 + 2 + 2), bewusst klein gehalten, weil sich der ganze Kurs ein
Kontingent von 100 vCPU teilt.

## Voraussetzungen

- VPN oder Eduroam aktiv
- Application Credential unter `~/.config/openstack/clouds.yaml`
- Terraform ≥ 1.5 installiert (`terraform version`)

## Anwenden

```bash
terraform init
terraform plan      # immer erst lesen, bevor du zustimmst
terraform apply
```

```bash
terraform output
ssh ubuntu@$(terraform output -raw master_ip)
```

## Größe ändern, die eine Falle dabei

Ein Resize kann die Root-Platte einer **bestehenden** Instanz nur vergrößern, nie verkleinern,
unabhängig von Flavor oder Image. Ein Wechsel auf einen Flavor mit weniger Platte scheitert mit:

```
Error: Flavor's disk is smaller than the minimum size specified in image metadata.
```

Betrifft das nur eine Vergrößerung, reicht `terraform apply` normal. Für eine Verkleinerung
braucht es einen Neubau:

```bash
terraform apply -replace='openstack_compute_instance_v2.worker[0]' \
                 -replace='openstack_compute_instance_v2.worker[1]'
```

Kostet neue IP-Adressen (Inventar wird automatisch neu geschrieben) und alles, was auf der
alten Platte lag, unkritisch, solange noch nichts deployt ist.

## Was hier abweicht, gegenüber der Kursvorlage

| | Vorlage aus dem Übungs-Track der Vorlesung | Hier | Warum |
|---|---|---|---|
| Anmeldung | Benutzer + Passwort | Application Credential (`clouds.yaml`, außerhalb des Repos) | SSO, kein Passwort mehr möglich |
| Netz | `DHBW-1-Upper` | `DHBWV6` | in der neuen Cloud umbenannt |
| Image | feste `image_id` | Auflösung über den Namen (`Ubuntu 24.04`, `most_recent`) | die IDs der Vorlage existieren nicht mehr; die DHBW ersetzt Images regelmäßig |
| Flavor | `m1.extra_large` (8 vCPU) für alle Knoten | `general.medium`/`general.small` | 3 × 8 vCPU je Gruppe erschöpften das übrige gemeinsame Kontingent |
| Adressen | `fixed_ip_v4` | `fixed_ip_v6`, Inventar mit `ip_family: dual` | die privaten IPv4 sind von außen nicht erreichbar |