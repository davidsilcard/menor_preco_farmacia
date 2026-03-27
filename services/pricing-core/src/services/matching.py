from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError

from src.models.base import CanonicalProduct, ProductMatch, RegulatoryProduct


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

        regulatory_product = self._find_regulatory_product(product_data)
        if regulatory_product:
            canonical = self._find_existing_canonical(
                ean_gtin or regulatory_product.ean_gtin,
                anvisa_code or regulatory_product.anvisa_code,
                regulatory_product=regulatory_product,
            )
            if canonical:
                return MatchDecision(
                    canonical,
                    "regulatory_anchor",
                    0.97,
                    "auto_approved",
                    "Produto ancorado por referencia regulatoria antes do matching textual.",
                )

        candidates = self.session.query(CanonicalProduct).filter_by(normalized_name=normalized_name).all()
        if len(candidates) == 1:
            candidate = candidates[0]
            same_brand = self._normalized(candidate.brand) == self._normalized(brand)
            same_dosage = self._normalized(candidate.dosage) == self._normalized(dosage)
            same_pack = self._normalized(candidate.pack_size) == self._normalized(pack_size)
            same_presentation = self._presentation_compatible(candidate.presentation, presentation)
            if not self._is_anchored(candidate):
                anchored_alternative = self._best_anchored_candidate(
                    normalized_name=normalized_name,
                    brand=brand,
                    dosage=dosage,
                    presentation=presentation,
                    pack_size=pack_size,
                    exclude_ids={candidate.id} if getattr(candidate, "id", None) else None,
                )

                if anchored_alternative:
                    best_score, best_candidate = anchored_alternative
                    return MatchDecision(
                        best_candidate,
                        "anchored_structured_match",
                        round(best_score, 2),
                        "auto_approved",
                        "Canonical ancorado por identificador forte prevaleceu sobre candidato legado sem identificador.",
                    )

            if same_brand and same_dosage and same_pack and same_presentation:
                if self._is_anchored(candidate):
                    return MatchDecision(
                        candidate,
                        "anchored_normalized_name",
                        0.96,
                        "auto_approved",
                        "Nome e atributos estruturados fecharam contra canonical ja ancorado por identificador forte.",
                    )
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
            best_score, best_candidate = attribute_candidates[0]
            if best_score >= 0.84 and self._is_anchored(best_candidate):
                return MatchDecision(
                    best_candidate,
                    "anchored_structured_match",
                    round(best_score, 2),
                    "auto_approved",
                    "Match estruturado contra canonical ja ancorado por identificador forte.",
                )
            return MatchDecision(
                best_candidate,
                "structured_name_similarity",
                round(best_score, 2),
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
        ean_gtin = self._clean_identifier(product_data.get("ean_gtin"))
        anvisa_code = product_data.get("anvisa_code")
        regulatory_product = self._find_regulatory_product(product_data)

        if regulatory_product:
            ean_gtin = ean_gtin or regulatory_product.ean_gtin
            anvisa_code = anvisa_code or regulatory_product.anvisa_code

        existing = self._find_existing_canonical(ean_gtin, anvisa_code, regulatory_product=regulatory_product)
        if existing:
            return existing

        canonical_name = product_data["raw_name"]
        normalized_name = product_data["normalized_name"]
        brand = product_data.get("brand")
        manufacturer = product_data.get("manufacturer")
        active_ingredient = product_data.get("active_ingredient")
        dosage = product_data.get("dosage")
        presentation = product_data.get("presentation")
        pack_size = product_data.get("pack_size")

        if regulatory_product:
            canonical_name = regulatory_product.product_name or canonical_name
            normalized_name = regulatory_product.normalized_product_name or normalized_name
            manufacturer = manufacturer or regulatory_product.manufacturer or regulatory_product.registration_holder
            active_ingredient = active_ingredient or regulatory_product.active_ingredient or regulatory_product.dcb_name
            dosage = dosage or regulatory_product.dosage or regulatory_product.concentration
            presentation = presentation or regulatory_product.presentation or regulatory_product.dosage_form
            pack_size = pack_size or regulatory_product.presentation

        canonical = CanonicalProduct(
            canonical_name=canonical_name,
            normalized_name=normalized_name,
            brand=brand,
            manufacturer=manufacturer,
            active_ingredient=active_ingredient,
            dosage=dosage,
            presentation=presentation,
            pack_size=pack_size,
            ean_gtin=ean_gtin,
            anvisa_code=anvisa_code,
        )
        self.session.add(canonical)
        try:
            self.session.flush()
            return canonical
        except IntegrityError:
            self.session.rollback()
            existing = self._find_existing_canonical(ean_gtin, anvisa_code)
            if existing:
                return existing
            raise

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
            if self._has_structural_variant_conflict(
                candidate_name=candidate.canonical_name or candidate.normalized_name,
                candidate_presentation=candidate.presentation,
                source_name=normalized_name,
                source_presentation=presentation,
            ):
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

    def _best_anchored_candidate(self, normalized_name, brand, dosage, presentation, pack_size, exclude_ids=None):
        exclude_ids = exclude_ids or set()
        ranked = self._rank_structured_candidates(normalized_name, brand, dosage, presentation, pack_size)
        anchored = [
            item
            for item in ranked
            if self._is_anchored(item[1]) and getattr(item[1], "id", None) not in exclude_ids
        ]
        if len(anchored) != 1:
            return None
        best_score, _ = anchored[0]
        if best_score < 0.72:
            return None
        return anchored[0]

    def _dosage_compatible(self, candidate, source):
        candidate_norm = self._normalized(candidate)
        source_norm = self._normalized(source)
        if not source_norm:
            return True
        return candidate_norm == source_norm

    def _presentation_compatible(self, candidate, source):
        if self._has_structural_variant_conflict(
            candidate_name=candidate,
            candidate_presentation=candidate,
            source_name=source,
            source_presentation=source,
        ):
            return False
        candidate_norm = self._presentation_key(candidate)
        source_norm = self._presentation_key(source)
        if not candidate_norm or not source_norm:
            return True
        return candidate_norm == source_norm

    @staticmethod
    def _is_anchored(candidate: CanonicalProduct):
        return bool(candidate.ean_gtin or candidate.anvisa_code)

    def _find_existing_canonical(self, ean_gtin, anvisa_code, regulatory_product=None):
        if ean_gtin:
            canonical = self.session.query(CanonicalProduct).filter_by(ean_gtin=ean_gtin).first()
            if canonical:
                return canonical
        if anvisa_code:
            canonical = self.session.query(CanonicalProduct).filter_by(anvisa_code=anvisa_code).first()
            if canonical:
                return canonical
        if regulatory_product:
            normalized_product_name = self._normalized(regulatory_product.normalized_product_name or regulatory_product.product_name)
            if normalized_product_name:
                canonical = self.session.query(CanonicalProduct).filter_by(normalized_name=normalized_product_name).first()
                if canonical:
                    return canonical
            for candidate in self.session.query(CanonicalProduct).all():
                if (
                    self._normalized(candidate.active_ingredient) == self._normalized(regulatory_product.active_ingredient or regulatory_product.dcb_name)
                    and self._dosage_compatible(candidate.dosage, regulatory_product.dosage or regulatory_product.concentration)
                    and self._presentation_compatible(candidate.presentation, regulatory_product.presentation or regulatory_product.dosage_form)
                ):
                    return candidate
        return None

    def _find_regulatory_product(self, product_data: dict):
        ean_gtin = self._clean_identifier(product_data.get("ean_gtin"))
        anvisa_code = self._clean_identifier(product_data.get("anvisa_code"))
        normalized_name = self._normalized(product_data.get("normalized_name"))
        active_ingredient = self._normalized(product_data.get("active_ingredient"))
        dosage = self._normalized(product_data.get("dosage"))
        presentation = self._normalized(product_data.get("presentation"))

        regulatory_products = self.session.query(RegulatoryProduct).all()
        if ean_gtin:
            for product in regulatory_products:
                if self._clean_identifier(product.ean_gtin) == ean_gtin:
                    return product
        if anvisa_code:
            for product in regulatory_products:
                if self._clean_identifier(product.anvisa_code) == anvisa_code:
                    return product
        for product in regulatory_products:
            if normalized_name and self._normalized(product.normalized_product_name or product.product_name) == normalized_name:
                return product
            product_active_ingredient = self._normalized(product.active_ingredient or product.dcb_name)
            product_dosage = self._normalized(product.dosage or product.concentration)
            product_presentation = self._normalized(product.presentation or product.dosage_form)
            if (
                active_ingredient
                and active_ingredient == product_active_ingredient
                and (not dosage or dosage == product_dosage)
                and self._presentation_compatible(presentation, product_presentation)
            ):
                return product
        return None

    @classmethod
    def _presentation_key(cls, value):
        normalized = cls._normalized(value)
        if not normalized:
            return normalized
        if "comprim" in normalized:
            return "comprimido"
        if "caps" in normalized:
            return "capsula"
        if "gota" in normalized:
            return "gotas"
        if "solucao" in normalized:
            return "solucao"
        if "supositorio" in normalized:
            return "supositorio"
        if "xarope" in normalized:
            return "xarope"
        return normalized

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

    @classmethod
    def _structural_variant_tokens(cls, *values):
        normalized = " ".join(filter(None, [cls._normalized(value) for value in values]))
        if not normalized:
            return set()
        variants = set()
        if any(token in normalized for token in [" xr ", " xr", "xr ", "liberacao prolongada", "prolongada", "extended release"]):
            variants.add("xr")
        if " dragea" in normalized or " drageas" in normalized:
            variants.add("dragea")
        return variants

    @classmethod
    def _has_structural_variant_conflict(cls, *, candidate_name, candidate_presentation, source_name, source_presentation):
        candidate_variants = cls._structural_variant_tokens(candidate_name, candidate_presentation)
        source_variants = cls._structural_variant_tokens(source_name, source_presentation)
        return candidate_variants != source_variants and bool(candidate_variants or source_variants)
