from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, model_validator


class EarthquakeSchema(BaseModel):
    event_id: Optional[str] = Field(default="", description="Earthquake id")
    date: Optional[str] = Field(default="", description="Earthquake date")
    time: Optional[str] = Field(default="", description="Earthquake time")
    latitude: Optional[float] = Field(default=0.0, description="Latitude")
    longitude: Optional[float] = Field(default=0.0, description="Longitude")
    depth: Optional[float] = Field(default=0.0, description="Depth (km)")
    magnitude: Optional[float] = Field(default=0.0, description="Magnitude")
    type: Optional[str] = Field(default="", description="Type (MD,ML,Mw)")
    location: Optional[str] = Field(default="", description="Location")

    city: Optional[str] = Field(default="", description="City")
    district: Optional[str] = Field(default="", description="District")
    neighborhood: Optional[str] = Field(default="", description="Neighborhood")

    solution_type: Optional[str] = Field(default="REVIZE", description="Solution type")
    source: Optional[str] = Field(default="", description="Source")

    @model_validator(mode="before")
    @classmethod
    def allow_anything_and_sanitize(cls, data: Any) -> Dict[str, Any]:
        """Dış kaynaktan gelen kirli veriyi temizler, None/bozuk tipleri güvenli tipe dönüştürür."""
        if not isinstance(data, dict):
            return {}

        # 1. String Alanlar Sanitization (None, int veya boolean gelse bile stringe güvenle çevir)
        str_fields = [
            "event_id", "date", "time", "type", "location",
            "city", "district", "neighborhood", "solution_type", "source"
        ]
        for field in str_fields:
            val = data.get(field)
            if val is None:
                data[field] = ""
            else:
                data[field] = str(val).strip()

        # 2. Numeric Alanlar Sanitization (String veya None gelse dahi float'a zorla, dönüştürülemezse 0.0 yap)
        float_fields = ["latitude", "longitude", "depth", "magnitude"]
        for field in float_fields:
            val = data.get(field)
            if val is None or val == "" or val == "-.-":
                data[field] = 0.0
            else:
                try:
                    data[field] = float(val)
                except (ValueError, TypeError):
                    data[field] = 0.0

        return data