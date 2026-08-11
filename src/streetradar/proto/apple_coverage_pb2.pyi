from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GroundMetadataTile(_message.Message):
    __slots__ = ("pano", "build_table", "camera_metadata", "tile_coordinate")
    class PhotoPosition(_message.Message):
        __slots__ = ("panoid", "revision", "timestamp", "build_table_idx", "camera_metadata_idx", "tile_position", "rig_metrics")
        class OrientedTilePosition(_message.Message):
            __slots__ = ("x", "y", "altitude", "yaw", "pitch", "roll")
            X_FIELD_NUMBER: _ClassVar[int]
            Y_FIELD_NUMBER: _ClassVar[int]
            ALTITUDE_FIELD_NUMBER: _ClassVar[int]
            YAW_FIELD_NUMBER: _ClassVar[int]
            PITCH_FIELD_NUMBER: _ClassVar[int]
            ROLL_FIELD_NUMBER: _ClassVar[int]
            x: int
            y: int
            altitude: int
            yaw: int
            pitch: int
            roll: int
            def __init__(self, x: _Optional[int] = ..., y: _Optional[int] = ..., altitude: _Optional[int] = ..., yaw: _Optional[int] = ..., pitch: _Optional[int] = ..., roll: _Optional[int] = ...) -> None: ...
        class RigMetrics(_message.Message):
            __slots__ = ("occlusion_score",)
            OCCLUSION_SCORE_FIELD_NUMBER: _ClassVar[int]
            occlusion_score: _containers.RepeatedScalarFieldContainer[int]
            def __init__(self, occlusion_score: _Optional[_Iterable[int]] = ...) -> None: ...
        PANOID_FIELD_NUMBER: _ClassVar[int]
        REVISION_FIELD_NUMBER: _ClassVar[int]
        TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
        BUILD_TABLE_IDX_FIELD_NUMBER: _ClassVar[int]
        CAMERA_METADATA_IDX_FIELD_NUMBER: _ClassVar[int]
        TILE_POSITION_FIELD_NUMBER: _ClassVar[int]
        RIG_METRICS_FIELD_NUMBER: _ClassVar[int]
        panoid: int
        revision: int
        timestamp: int
        build_table_idx: int
        camera_metadata_idx: _containers.RepeatedScalarFieldContainer[int]
        tile_position: GroundMetadataTile.PhotoPosition.OrientedTilePosition
        rig_metrics: GroundMetadataTile.PhotoPosition.RigMetrics
        def __init__(self, panoid: _Optional[int] = ..., revision: _Optional[int] = ..., timestamp: _Optional[int] = ..., build_table_idx: _Optional[int] = ..., camera_metadata_idx: _Optional[_Iterable[int]] = ..., tile_position: _Optional[_Union[GroundMetadataTile.PhotoPosition.OrientedTilePosition, _Mapping]] = ..., rig_metrics: _Optional[_Union[GroundMetadataTile.PhotoPosition.RigMetrics, _Mapping]] = ...) -> None: ...
    class GroundDataBuild(_message.Message):
        __slots__ = ("index", "build_id", "data_format_version", "coverage_type", "bucket_id", "lod_with_textures", "metrics_version", "data_output_version")
        class CoverageType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            DEFAULT: _ClassVar[GroundMetadataTile.GroundDataBuild.CoverageType]
            CAR: _ClassVar[GroundMetadataTile.GroundDataBuild.CoverageType]
            BACKPACK: _ClassVar[GroundMetadataTile.GroundDataBuild.CoverageType]
        DEFAULT: GroundMetadataTile.GroundDataBuild.CoverageType
        CAR: GroundMetadataTile.GroundDataBuild.CoverageType
        BACKPACK: GroundMetadataTile.GroundDataBuild.CoverageType
        INDEX_FIELD_NUMBER: _ClassVar[int]
        BUILD_ID_FIELD_NUMBER: _ClassVar[int]
        DATA_FORMAT_VERSION_FIELD_NUMBER: _ClassVar[int]
        COVERAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
        BUCKET_ID_FIELD_NUMBER: _ClassVar[int]
        LOD_WITH_TEXTURES_FIELD_NUMBER: _ClassVar[int]
        METRICS_VERSION_FIELD_NUMBER: _ClassVar[int]
        DATA_OUTPUT_VERSION_FIELD_NUMBER: _ClassVar[int]
        index: int
        build_id: int
        data_format_version: int
        coverage_type: GroundMetadataTile.GroundDataBuild.CoverageType
        bucket_id: int
        lod_with_textures: int
        metrics_version: int
        data_output_version: int
        def __init__(self, index: _Optional[int] = ..., build_id: _Optional[int] = ..., data_format_version: _Optional[int] = ..., coverage_type: _Optional[_Union[GroundMetadataTile.GroundDataBuild.CoverageType, str]] = ..., bucket_id: _Optional[int] = ..., lod_with_textures: _Optional[int] = ..., metrics_version: _Optional[int] = ..., data_output_version: _Optional[int] = ...) -> None: ...
    PANO_FIELD_NUMBER: _ClassVar[int]
    BUILD_TABLE_FIELD_NUMBER: _ClassVar[int]
    CAMERA_METADATA_FIELD_NUMBER: _ClassVar[int]
    TILE_COORDINATE_FIELD_NUMBER: _ClassVar[int]
    pano: _containers.RepeatedCompositeFieldContainer[GroundMetadataTile.PhotoPosition]
    build_table: _containers.RepeatedCompositeFieldContainer[GroundMetadataTile.GroundDataBuild]
    camera_metadata: _containers.RepeatedCompositeFieldContainer[CameraMetadata]
    tile_coordinate: TileCoordinate
    def __init__(self, pano: _Optional[_Iterable[_Union[GroundMetadataTile.PhotoPosition, _Mapping]]] = ..., build_table: _Optional[_Iterable[_Union[GroundMetadataTile.GroundDataBuild, _Mapping]]] = ..., camera_metadata: _Optional[_Iterable[_Union[CameraMetadata, _Mapping]]] = ..., tile_coordinate: _Optional[_Union[TileCoordinate, _Mapping]] = ...) -> None: ...

class CameraMetadata(_message.Message):
    __slots__ = ("camera_number", "lens_projection", "position", "texture_id")
    class LensProjection(_message.Message):
        __slots__ = ("type", "fov_s", "fov_h", "k2", "k3", "k4", "cx", "cy", "lx", "ly")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        FOV_S_FIELD_NUMBER: _ClassVar[int]
        FOV_H_FIELD_NUMBER: _ClassVar[int]
        K2_FIELD_NUMBER: _ClassVar[int]
        K3_FIELD_NUMBER: _ClassVar[int]
        K4_FIELD_NUMBER: _ClassVar[int]
        CX_FIELD_NUMBER: _ClassVar[int]
        CY_FIELD_NUMBER: _ClassVar[int]
        LX_FIELD_NUMBER: _ClassVar[int]
        LY_FIELD_NUMBER: _ClassVar[int]
        type: int
        fov_s: float
        fov_h: float
        k2: float
        k3: float
        k4: float
        cx: float
        cy: float
        lx: float
        ly: float
        def __init__(self, type: _Optional[int] = ..., fov_s: _Optional[float] = ..., fov_h: _Optional[float] = ..., k2: _Optional[float] = ..., k3: _Optional[float] = ..., k4: _Optional[float] = ..., cx: _Optional[float] = ..., cy: _Optional[float] = ..., lx: _Optional[float] = ..., ly: _Optional[float] = ...) -> None: ...
    class OrientedPosition(_message.Message):
        __slots__ = ("x", "y", "z", "yaw", "pitch", "roll")
        X_FIELD_NUMBER: _ClassVar[int]
        Y_FIELD_NUMBER: _ClassVar[int]
        Z_FIELD_NUMBER: _ClassVar[int]
        YAW_FIELD_NUMBER: _ClassVar[int]
        PITCH_FIELD_NUMBER: _ClassVar[int]
        ROLL_FIELD_NUMBER: _ClassVar[int]
        x: float
        y: float
        z: float
        yaw: float
        pitch: float
        roll: float
        def __init__(self, x: _Optional[float] = ..., y: _Optional[float] = ..., z: _Optional[float] = ..., yaw: _Optional[float] = ..., pitch: _Optional[float] = ..., roll: _Optional[float] = ...) -> None: ...
    CAMERA_NUMBER_FIELD_NUMBER: _ClassVar[int]
    LENS_PROJECTION_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    TEXTURE_ID_FIELD_NUMBER: _ClassVar[int]
    camera_number: int
    lens_projection: CameraMetadata.LensProjection
    position: CameraMetadata.OrientedPosition
    texture_id: int
    def __init__(self, camera_number: _Optional[int] = ..., lens_projection: _Optional[_Union[CameraMetadata.LensProjection, _Mapping]] = ..., position: _Optional[_Union[CameraMetadata.OrientedPosition, _Mapping]] = ..., texture_id: _Optional[int] = ...) -> None: ...

class TileCoordinate(_message.Message):
    __slots__ = ("x", "y", "z")
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    Z_FIELD_NUMBER: _ClassVar[int]
    x: int
    y: int
    z: int
    def __init__(self, x: _Optional[int] = ..., y: _Optional[int] = ..., z: _Optional[int] = ...) -> None: ...
