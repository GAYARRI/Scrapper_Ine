Param(
  [switch]$Recreate
)

$venv = ".venv"
if ($Recreate -and (Test-Path $venv)) { Remove-Item -Recurse -Force $venv }

if (-not (Test-Path $venv)) {
  Write-Host "No existe .venv. Creándolo..."
  py -m venv .venv
}

$activate = ".\.venv\Scripts\Activate.ps1"
if (-not (Test-Path $activate)) { throw "No se encontró $activate" }

Write-Host "Activando entorno..."
. $activate
python -V
Write-Host "Usa 'deactivate' para salir."
