param(
    [string]$BaseUrl = "http://127.0.0.1:8001",
    [switch]$FullMatrix,
    [string]$OutputCsvPath = "",
    [string]$OutputJsonPath = ""
)

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [Console]::OutputEncoding

function Invoke-SearchCase {
    param(
        [string]$Query,
        [string]$Cep
    )

    $url = "$BaseUrl/tool/search-products?query=$([uri]::EscapeDataString($Query))&cep=$Cep"
    try {
        $r = Invoke-RestMethod $url
        $first = $null
        if ($r.result.results_count -gt 0) {
            $first = $r.result.results[0]
        }

        return [pscustomobject]@{
            type = "search"
            query = $Query
            cep = $Cep
            resolution_source = $r.result.resolution_source
            outcome = $r.result.outcome
            evidence_level = $r.result.evidence_level
            requires_polling = $r.result.requires_polling
            results_count = $r.result.results_count
            offers_count = $r.result.offers_count
            search_job_id = $r.result.search_job_id
            operation_job_id = $r.result.operation_job_id
            tracked_item_id = $r.result.tracked_item_id
            first_canonical = if ($first) { $first.canonical_name } else { $null }
            first_offer_count = if ($first) { @($first.offers).Count } else { 0 }
            warnings = ($r.warnings -join " | ")
            error_message = $null
        }
    }
    catch {
        return [pscustomobject]@{
            type = "search"
            query = $Query
            cep = $Cep
            resolution_source = $null
            outcome = $null
            evidence_level = $null
            requires_polling = $false
            results_count = 0
            offers_count = 0
            search_job_id = $null
            operation_job_id = $null
            tracked_item_id = $null
            first_canonical = $null
            first_offer_count = 0
            warnings = $null
            error_message = $_.Exception.Message
        }
    }
}

function Invoke-OpsCase {
    param(
        [string]$Cep
    )

    $url = "$BaseUrl/ops/metrics?cep=$Cep"
    try {
        $r = Invoke-RestMethod $url
        return [pscustomobject]@{
            type = "ops"
            cep = $Cep
            requested_cep = $r.requested_cep
            configured_default_cep = $r.configured_default_cep
            source_products = $r.catalog.source_products
            latest_snapshots = $r.catalog.latest_snapshots
            tracked_items = $r.tracked_items.total
            queued_jobs = $r.queue.queued_jobs
            error_message = $null
        }
    }
    catch {
        return [pscustomobject]@{
            type = "ops"
            cep = $Cep
            requested_cep = $null
            configured_default_cep = $null
            source_products = $null
            latest_snapshots = $null
            tracked_items = $null
            queued_jobs = $null
            error_message = $_.Exception.Message
        }
    }
}

function Get-SearchClassification {
    param(
        [psobject]$Case
    )

    if ($Case.error_message) {
        return "falha_tecnica"
    }
    if ($Case.evidence_level -eq "real_offer") {
        return "sucesso_real"
    }
    if ($Case.evidence_level -in @("source_product", "canonical_only")) {
        return "sucesso_parcial_com_fallback"
    }
    if ($Case.outcome -eq "queued") {
        return "sucesso_parcial_com_fallback"
    }
    if ($Case.outcome -eq "resolved" -and $Case.results_count -gt 0) {
        return "inconclusivo"
    }
    return "falha_funcional"
}

function Write-Section {
    param(
        [string]$Title
    )

    Write-Host ""
    Write-Host "=== $Title ==="
}

$coreSearchCases = @(
    @{ Query = "buscopan composto"; Cep = "89254300"; Label = "Caso 1: oferta real" },
    @{ Query = "dipirona"; Cep = "89254300"; Label = "Caso 2: source product fallback" },
    @{ Query = "produto raro xyz 2026"; Cep = "89252000"; Label = "Caso 3: fila" },
    @{ Query = "buscopan composto"; Cep = "89251000"; Label = "Caso 4: match sem oferta" }
)

$opsCases = @(
    @{ Cep = "89251000"; Label = "Caso 5: leitura operacional por CEP" }
)

$matrixSearchCases = @()
if ($FullMatrix) {
    $ceps = @("89254300", "89251000", "89252000", "89253000", "89254000")
    $meds = @(
        "buscopan composto",
        "dipirona",
        "dipirona 500mg",
        "jardiance 25mg",
        "clonazepam gotas 20ml",
        "amoxicilina 500mg",
        "neosaldina drageas",
        "oxcarbazepina 600mg"
    )
    foreach ($cep in $ceps) {
        foreach ($med in $meds) {
            $matrixSearchCases += @{ Query = $med; Cep = $cep; Label = "Matriz" }
        }
    }
}

$searchResults = @()
foreach ($case in $coreSearchCases + $matrixSearchCases) {
    $result = Invoke-SearchCase -Query $case.Query -Cep $case.Cep
    $result | Add-Member -NotePropertyName label -NotePropertyValue $case.Label
    $result | Add-Member -NotePropertyName classification -NotePropertyValue (Get-SearchClassification -Case $result)
    $searchResults += $result
}

$opsResults = @()
foreach ($case in $opsCases) {
    $result = Invoke-OpsCase -Cep $case.Cep
    $result | Add-Member -NotePropertyName label -NotePropertyValue $case.Label
    $opsResults += $result
}

Write-Section "Casos Principais"
$searchResults | Where-Object { $_.label -ne "Matriz" } | Format-List

Write-Section "Leitura Operacional"
$opsResults | Format-List

if ($FullMatrix) {
    Write-Section "Resumo da Matriz"
    $searchResults |
        Group-Object classification |
        Select-Object Name, Count |
        Sort-Object Name |
        Format-Table -AutoSize

    Write-Section "Tabela da Matriz"
    $searchResults |
        Select-Object query, cep, resolution_source, outcome, evidence_level, results_count, offers_count, search_job_id, classification |
        Sort-Object cep, query |
        Format-Table -AutoSize
}

if ($OutputCsvPath) {
    $searchResults | Export-Csv -NoTypeInformation -Encoding UTF8 $OutputCsvPath
}

if ($OutputJsonPath) {
    [pscustomobject]@{
        search_results = $searchResults
        ops_results = $opsResults
    } | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $OutputJsonPath
}
