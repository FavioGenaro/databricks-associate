# Instructivo: Setup inicial de Azure Databricks

Este instructivo es para cada alumno del curso, que lo sigue en su propia suscripción de Azure, en paralelo al workspace `dbassociate` que usa el docente para dictar las sesiones. Cubre la creación del grupo de recursos, del workspace de Azure Databricks y de la estructura básica de Unity Catalog (catalog `dbassociate`, schemas `bronze`/`silver`/`gold` y el volume de landing) que los labs de las sesiones dan por existente.

---

## Prerrequisitos

- Cuenta de Azure con una suscripción activa (Azure Free Trial o suscripción propia).
- Permisos de **Contributor** u **Owner** sobre la suscripción, o al menos sobre el grupo de recursos que se va a crear.
- Acceso al [Azure Portal](https://portal.azure.com).

---

## Paso 1: Crear el grupo de recursos

1. Iniciar sesión en el [Azure Portal](https://portal.azure.com).
2. Buscar **Resource groups** en la barra de búsqueda superior y hacer click en **+ Create**.
3. Completar:

   | Campo | Valor |
   |---|---|
   | **Subscription** | La suscripción de Azure del alumno |
   | **Resource group** | Nombre descriptivo, ej. `rg-dbassociate-<nombre-alumno>` |
   | **Region** | A elección del alumno, usar la misma región en el Paso 2 |

4. **Review + create** → **Create**.

---

## Paso 2: Crear el workspace de Azure Databricks

1. En el Azure Portal, buscar **Azure Databricks** y hacer click en **+ Create**.
2. Completar la pestaña **Basics**:

   | Campo | Valor |
   |---|---|
   | **Subscription** | La misma suscripción del Paso 1 |
   | **Resource group** | El grupo de recursos creado en el Paso 1 |
   | **Workspace name** | Nombre descriptivo, ej. `dbw-dbassociate-<nombre-alumno>` |
   | **Region** | La misma región elegida en el Paso 1; workspace y grupo de recursos deben coincidir |
   | **Pricing Tier** | **Premium**: el curso usa Unity Catalog desde la Sesión 01, y Unity Catalog requiere el plan Premium |
   | **Workspace type** | **Hybrid** (workspace clásico): el curso usa job clusters y all-purpose compute clásico además de serverless, algo que el tipo Serverless no cubre |
   | **Managed Resource Group name** | Solo aparece con workspace type Hybrid. Opcional: nombre propio para el resource group administrado que Databricks crea automáticamente, ej. `mrg-dbassociate-<nombre-alumno>`. Si se deja vacío, Azure genera uno automáticamente |

3. **Review + create** → **Create**. El despliegue toma unos minutos.
4. Al finalizar, click en **Go to resource** → **Launch Workspace** para confirmar que el workspace abre correctamente.

---

## Paso 3: Verificar y configurar Unity Catalog

### 3.1 Verificar que el workspace esté habilitado para Unity Catalog

Los workspaces de Azure Databricks creados después del 9 de noviembre de 2023 quedan habilitados para Unity Catalog automáticamente, con un metastore ya asignado por región. Para confirmarlo:

1. Dentro del workspace, click en el ícono **Catalog** en la barra lateral.
2. Si aparece un catalog con el nombre del workspace bajo **Catalogs**, Unity Catalog ya está habilitado y no hace falta crear un metastore manualmente.
3. Alternativa: abrir un notebook o el SQL editor y ejecutar `SELECT CURRENT_METASTORE();`. Si devuelve un ID de metastore, el workspace está habilitado.

Si esta verificación no devuelve un metastore, seguir los pasos 3.1.a a 3.1.c para crearlo y asignarlo al workspace. Se necesitan permisos de account admin, que el alumno ya tiene por ser quien creó la cuenta de Azure Databricks al desplegar el workspace en el Paso 2.

#### 3.1.a Crear el storage account para el metastore

1. En el Azure Portal, crear un **Storage account** nuevo en el mismo grupo de recursos y región de los Pasos 1 y 2, con **Hierarchical namespace** habilitado (pestaña **Advanced**), para que funcione como Azure Data Lake Storage Gen2.
2. Dentro del storage account, crear un **Container** (ej. `unity-catalog-metastore`).

#### 3.1.b Crear el Access Connector y darle acceso al storage

1. En el Azure Portal, buscar **Access Connector for Azure Databricks** y click en **+ Create**.
2. Completar: **Subscription** y **Resource group** (los mismos del Paso 1), **Name** (ej. `ac-dbassociate-<nombre-alumno>`), **Region** (la misma del storage account).
3. En la pestaña **Managed Identity**, dejar **System assigned** en **On**.
4. **Review + create** → **Create**. Al finalizar, abrir el recurso y copiar su **Resource ID** (formato `/subscriptions/<id>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/<nombre>`).
5. Ir al storage account creado en 3.1.a → **Access Control (IAM)** → **+ Add** → **Add role assignment**.
6. Rol: **Storage Blob Data Contributor**. En **Assign access to**, seleccionar **Managed identity** → **+ Select members** → buscar y elegir el Access Connector creado → **Review + assign**.

#### 3.1.c Crear el metastore y asignarlo al workspace

1. Desde el workspace, click en el nombre de usuario (arriba a la derecha) → **Manage Account**, para abrir el account console de Azure Databricks.
2. Click en **Catalog** → **Create metastore**.
3. Completar:

   | Campo | Valor |
   |---|---|
   | **Name** | `metastore-<nombre-alumno>` |
   | **Region** | La misma región de los Pasos 1 y 2 |
   | **ADLS Gen 2 path** | `abfss://<container>@<storage-account>.dfs.core.windows.net/`, con el container y storage account del Paso 3.1.a |
   | **Access Connector ID** | El Resource ID copiado en el Paso 3.1.b |

4. Click en **Create**.
5. Cuando se solicite seleccionar workspaces, elegir el workspace creado en el Paso 2 y click en **Assign**. Esto habilita Unity Catalog para ese workspace.

Con el metastore asignado, continuar con el Paso 3.2.

### 3.2 Crear el catalog `dbassociate`

1. Click en **Catalog** en la barra lateral, luego en **Catalogs** bajo **Quick access**.
2. Click en **Create catalog**.
3. Completar:

   | Campo | Valor |
   |---|---|
   | **Catalog name** | `dbassociate` |
   | **Type** | Standard |
   | **Storage location** | Dejar en blanco (usa el almacenamiento por defecto del metastore, suficiente para este curso) |

4. Click en **Create**. Al crearse, Unity Catalog agrega automáticamente los schemas `default` e `information_schema`.

### 3.3 Crear los schemas `bronze`, `silver` y `gold`

Dentro del catalog `dbassociate`:

1. Click en **Create schema**.
2. Nombre del schema: `bronze`. Click en **Create**.
3. Repetir para `silver` y para `gold`.

Al finalizar, el catalog `dbassociate` debe tener cinco schemas: `default`, `information_schema`, `bronze`, `silver`, `gold`.

### 3.4 Crear el volume de landing

1. Dentro del schema `dbassociate.default`, click en **Create volume**.
2. Nombre del volume: `vol_landing`. Tipo: Managed.
3. Dentro del volume ya creado, click en **Create directory** y crear la subcarpeta `sesion_01` (ver convención de subcarpeta por sesión).
4. Subir `ventas_demo.csv` a `vol_landing/sesion_01/`: **Add data** → **Upload files to a volume**, seleccionar el directorio `sesion_01` como destino.
