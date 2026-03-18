from dataclasses import dataclass

from src.models.base import CanonicalProduct, ProductMatch


@dataclass
class MatchDecision:
    canonical_product: CanonicalProduct | None
    match_type: str
    confidence: float
    review_status: str
    review_notes: str | None = None


class ProductMatcher:
    def __init__(self, session):
        self.session = session

    def match_source_product(self, product_data: dict) -> MatchDecision:
        ean_gtin = self._clean_identifier(product_data.get("ean_gtin"))
        anvisa_code = product_data.get("anvisa_code")
        normalized_name = product_data["normalized_name"]
        brand = product_data.get("brand")
        dosage = product_data.get("dosage")
        presentation = product_data.get("presentation")
        pack_size = product_data.get("pack_size")

        if ean_gtin:
            canonical = self.session.query(CanonicalProduct).filter_by(ean_gtin=ean_gtin).first()
            if canonical:
                return MatchDecision(canonical, "ean_gtin", 1.0, "auto_approved")

        if anvisa_code:
            canonical = self.session.query(CanonicalProduct).filter_by(anvisa_code=anvisa_code).first()
            if canonical:
                return MatchDecision(canonical, "anvisa_code", 0.98, "auto_approved")

        candidates = self.session.query(CanonicalProduct).filter_by(normalized_name=normalized_name).all()
        if len(candidates) == 1:
            candidate = candidates[0]
            same_brand = self._normalized(candidate.brand) == self._normalized(brand)
            same_dosage = self._normalized(candidate.dosage) == self._normalized(dosage)
            same_pack = self._normalized(candidate.pack_size) == self._normalized(pack_size)

            if same_brand and same_dosage and same_pack:
                return MatchDecision(candidate, "normalized_name_strict", 0.9, "needs_review")

            return MatchDecision(
                candidate,
                "normalized_name_loose",
                0.6,
                "needs_review",
                "Nome normalizado bateu, mas atributos estruturados nao fecharam completamente.",
            )

        attribute_candidates = self._rank_structured_candidates(
            normalized_name=normalized_name,
            brand=brand,
            dosage=dosage,
            presentation=presentation,
            pack_size=pack_size,
        )
        if len(attribute_candidates) == 1:
            return MatchDecision(
                attribute_candidates[0][1],
                "structured_name_similarity",
                round(attribute_candidates[0][0], 2),
                "needs_review",
                "Nome proximo e atributos estruturados bateram; conferir identificadores da origem.",
            )
        if len(attribute_candidates) > 1:
            return MatchDecision(
                None,
                "ambiguous_structured_match",
                0.0,
                "needs_review",
                "Mais de um candidato estruturado forte encontrado; revisar manualmente.",
            )

        return MatchDecision(
            None,
            "new_canonical",
            0.0,
            "new",
            "Nenhum candidato forte encontrado; criar canonical novo.",
        )

    def build_canonical_product(self, product_data: dict) -> CanonicalProduct:
        canonical = CanonicalProduct(
            canonical_name=product_data["raw_name"],
            normalized_name=product_data["normalized_name"],
            brand=product_data.get("brand"),
            manufacturer=product_data.get("manufacturer"),
            active_ingredient=product_data.get("active_ingredient"),
            dosage=product_data.get("dosage"),
            presentation=product_data.get("presentation"),
            pack_size=product_data.get("pack_size"),
            ean_gtin=self._clean_identifier(product_data.get("ean_gtin")),
            anvisa_code=product_data.get("anvisa_code"),
        )
        self.session.add(canonical)
        self.session.flush()
        return canonical

    def resolve_match_metadata(self, canonical_product: CanonicalProduct, product_data: dict) -> MatchDecision:
        ean_gtin = self._clean_identifier(product_data.get("ean_gtin"))
        anvisa_code = product_data.get("anvisa_code")

        if ean_gtin and canonical_product.ean_gtin and canonical_product.ean_gtin == ean_gtin:
            return MatchDecision(canonical_product, "ean_gtin", 1.0, "auto_approved")

        if anvisa_code and canonical_product.anvisa_code and canonical_product.anvisa_code == anvisa_code:
            return MatchDecision(canonical_product, "anvisa_code", 0.98, "auto_approved")

        return self.match_source_product(product_data)

    def reconcile_canonical_matches(self, canonical_product: CanonicalProduct):
        if canonical_product.ean_gtin:
            matches = self.session.query(ProductMatch).filter_by(canonical_product_id=canonical_product.id).all()
            for match in matches:
                source_product = match.source_product
                if source_product and source_product.ean_gtin == canonical_product.ean_gtin:
                    match.match_type = "ean_gtin"
                    match.confidence = 1.0
                    match.review_status = "auto_approved"
                    match.review_notes = None

        if canonical_product.anvisa_code:
            matches = self.session.query(ProductMatch).filter_by(canonical_product_id=canonical_product.id).all()
            for match in matches:
                source_product = match.source_product
                if source_product and source_product.anvisa_code == canonical_product.anvisa_code:
                    match.match_type = "anvisa_code"
                    match.confidence = 0.98
                    match.review_status = "auto_approved"
                    match.review_notes = None

    @staticmethod
    def _normalized(value):
        return (value or "").strip().lower()

    @staticmethod
    def _clean_identifier(value):
        if not value:
            return None
        digits = "".join(ch for ch in str(value) if ch.isdigit())
        if digits.startswith("999"):
            return None
        return digits or None

    def _name_compatible(self, left, right):
        left_tokens = self._significant_tokens(left)
        right_tokens = self._significant_tokens(right)
        if not left_tokens or not right_tokens:
            return False
        special_tokens = {
            "efervescentes",
            "efervescente",
            "gotas",
            "gota",
            "supositorio",
            "supositorios",
            "solucao",
            "oral",
            "flash",
            "seringa",
        }
        if any(((token in left_tokens) != (token in right_tokens)) for token in special_tokens):
            return False
        smaller, larger = (left_tokens, right_tokens) if len(left_tokens) <= len(right_tokens) else (right_tokens, left_tokens)
        overlap = smaller.intersection(larger)
        return len(overlap) >= 2 and overlap == smaller

    def _rank_structured_candidates(self, normalized_name, brand, dosage, presentation, pack_size):
        ranked = []
        for candidate in self.session.query(CanonicalProduct).all():
            if not self._name_compatible(candidate.normalized_name, normalized_name):
                continue
            if self._normalized(candidate.pack_size) != self._normalized(pack_size):
                continue
            if not self._presentation_compatible(candidate.presentation, presentation):
                continue
            if not self._dosage_compatible(candidate.dosage, dosage):
                continue

            score = 0.72
            if self._normalized(candidate.brand) == self._normalized(brand) and self._normalized(brand):
                score += 0.08
            if self._normalized(candidate.dosage) == self._normalized(dosage) and self._normalized(dosage):
                score += 0.08
            if self._normalized(candidate.presentation) == self._normalized(presentation) and self._normalized(presentation):
                score += 0.06

            ranked.append((min(score, 0.94), candidate))

        if not ranked:
            return []

        ranked.sort(key=lambda item: item[0], reverse=True)
        best_score = ranked[0][0]
        if len(ranked) == 1:
            return ranked

        second_score = ranked[1][0]
        if best_score - second_score < 0.05:
            return [item for item in ranked if abs(item[0] - best_score) < 0.05]
        return [ranked[0]]

    def _dosage_compatible(self, candidate, source):
        candidate_norm = self._normalized(candidate)
        source_norm = self._normalized(source)
        if not source_norm:
            return True
        return candidate_norm == source_norm

    def _presentation_compatible(self, candidate, source):
        candidate_norm = self._normalized(candidate)
        source_norm = self._normalized(source)
        if not candidate_norm or not source_norm:
            return True
        return candidate_norm == source_norm

    @classmethod
    def _significant_tokens(cls, value):
        normalized = cls._normalized(value)
        if not normalized:
            return set()
        raw_tokens = [token for token in normalized.replace("/", " ").split() if token]
        stopwords = {
            "analgesico",
            "antitermico",
            "adulto",
            "para",
            "de",
            "e",
            "monoidratada",
            "framboesa",
        }
        return {token for token in raw_tokens if token not in stopwords}
