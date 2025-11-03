function tooltip(d) {
  if (!d || !d.object) return null;

  const obj = d.object;

  // Extraction robuste de la valeur (brute)
  let value =
    obj.completude ??
    obj.__formatted__?.completude ??
    d.extra_json?.completude ??
    null;

  // Formatage en pourcentage
  if (value !== null && !isNaN(value)) {
    value = (Number(value) * 100).toFixed(2) + "%";
  } else {
    value = "N/A";
  }

  // Extraction Région & District
  const region = obj.region ?? d.extra_json?.region ?? "—";
  const district = obj.district ?? d.extra_json?.district ?? "—";

  return `
    <div style="font-family: Verdana; font-size: 12px; line-height: 1.4;">
      <span><strong>Région:</strong> ${region}</span><br>
      <span><strong>District:</strong> ${district}</span><br>
      <span><strong>Taux de complétude:</strong> <strong>${value}</strong></span>
    </div>
  `;
}


function tooltip(d) {
  if (!d || !d.object) return null;

  const obj = d.object;

  // Extraction robuste de la valeur (brute)
  let value =
    obj.completude ??
    obj.__formatted__?.completude ??
    d.extra_json?.completude ??
    null;

  // Formatage en pourcentage
  if (value !== null && !isNaN(value)) {
    value = (Number(value) * 100).toFixed(2) + "%";
  } else {
    value = "N/A";
  }

  const region = obj.region ?? d.extra_json?.region ?? "—";
  const district = obj.district ?? d.extra_json?.district ?? "—";

  return `
    <div style="
      font-family: Verdana;
      font-size: 12px;
      line-height: 1.4;
      background-color: #f2f2f2;     /* ✅ Gris clair */
      padding: 8px 10px;             /* ✅ Espace interne */
      border-radius: 6px;            /* ✅ Coins arrondis */
    ">
      <span><strong>Région:</strong> ${region}</span><br>
      <span><strong>District:</strong> ${district}</span><br>
      <span><strong>Taux de complétude:</strong> <strong>${value}</strong></span>
    </div>
  `;
}
