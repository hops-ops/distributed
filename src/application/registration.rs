use std::sync::Arc;

use super::error::ApplicationResult;
use super::manifest::{ApplicationExtension, ApplicationManifest, ManifestProvenance};
use super::module::Module;
use super::surface::SurfaceSpec;
use crate::graphql::surface::Surface;
use crate::graphql::{
    ClientManifestError, DistributedClientManifest, DistributedClientSurfaceExport,
};

/// Explicit application registration. No linker inventory or source scan is
/// consulted; only the values supplied to this constructor participate.
pub struct Application {
    name: String,
    modules: Vec<Module>,
    surfaces: Vec<SurfaceSpec>,
    manifest: ApplicationManifest,
}

impl Clone for Application {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            modules: self.modules.clone(),
            surfaces: self.surfaces.clone(),
            manifest: self.manifest.clone(),
        }
    }
}

impl std::fmt::Debug for Application {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Application")
            .field("name", &self.name)
            .field(
                "modules",
                &self.modules.iter().map(Module::id).collect::<Vec<_>>(),
            )
            .field("surfaces", &self.surfaces.len())
            .finish()
    }
}

impl Application {
    pub fn new(name: impl Into<String>) -> ApplicationBuilder {
        ApplicationBuilder::new(name)
    }

    pub fn try_new(
        name: impl Into<String>,
        modules: impl IntoIterator<Item = Module>,
        surfaces: impl IntoIterator<Item = SurfaceSpec>,
    ) -> ApplicationResult<Self> {
        let modules = modules.into_iter().collect::<Vec<_>>();
        let surfaces = surfaces.into_iter().collect::<Vec<_>>();
        let manifest =
            ApplicationManifest::try_from_modules(name, modules.clone(), surfaces.clone())?;
        Ok(Self {
            name: manifest.name.clone(),
            modules,
            surfaces,
            manifest,
        })
    }

    /// Declare a gateway's logical capabilities beside the typed Service surface.
    /// Origins, routes, credentials and adapter resources remain host bindings.
    #[cfg(feature = "gateway")]
    pub fn with_gateway(
        mut self,
        id: impl Into<String>,
        gateway: &crate::gateway::Gateway,
    ) -> ApplicationResult<Self> {
        self.manifest
            .extensions
            .push(gateway.application_extension(id)?);
        self.manifest.refresh_fingerprints()?;
        Ok(self)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn modules(&self) -> &[Module] {
        &self.modules
    }

    pub fn surfaces(&self) -> &[SurfaceSpec] {
        &self.surfaces
    }

    pub fn manifest(&self) -> &ApplicationManifest {
        &self.manifest
    }

    pub fn canonical_bytes(&self) -> ApplicationResult<Vec<u8>> {
        self.manifest.canonical_bytes()
    }

    pub fn fingerprint(&self) -> ApplicationResult<String> {
        self.manifest.fingerprint()
    }
}

/// Fluent explicit application authoring API.
pub struct ApplicationBuilder {
    name: String,
    modules: Vec<Module>,
    surfaces: Vec<SurfaceSpec>,
    extensions: Vec<ApplicationExtension>,
    provenance: Option<ManifestProvenance>,
}

impl ApplicationBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            modules: Vec::new(),
            surfaces: Vec::new(),
            extensions: Vec::new(),
            provenance: None,
        }
    }

    pub fn module(mut self, module: Module) -> Self {
        self.modules.push(module);
        self
    }

    pub fn modules(mut self, modules: impl IntoIterator<Item = Module>) -> Self {
        self.modules.extend(modules);
        self
    }

    pub fn surface(mut self, surface: SurfaceSpec) -> Self {
        self.surfaces.push(surface);
        self
    }

    pub fn surfaces(mut self, surfaces: impl IntoIterator<Item = SurfaceSpec>) -> Self {
        self.surfaces.extend(surfaces);
        self
    }

    pub fn extension(mut self, extension: ApplicationExtension) -> Self {
        self.extensions.push(extension);
        self
    }

    pub fn extensions(
        mut self,
        extensions: impl IntoIterator<Item = ApplicationExtension>,
    ) -> Self {
        self.extensions.extend(extensions);
        self
    }

    pub fn provenance(mut self, provenance: ManifestProvenance) -> Self {
        self.provenance = Some(provenance);
        self
    }

    pub fn build(self) -> ApplicationResult<Application> {
        let mut application = Application::try_new(self.name, self.modules, self.surfaces)?;
        application.manifest.extensions.extend(self.extensions);
        if let Some(provenance) = self.provenance {
            application.manifest = application.manifest.with_provenance(provenance);
        }
        application.manifest.refresh_fingerprints()?;
        Ok(application)
    }
}

/// Pure compiler entrypoint for contract-only packages.
pub struct ContractCompiler {
    name: String,
    modules: Vec<Module>,
    surface: Option<Arc<Surface>>,
    surface_spec: Option<SurfaceSpec>,
}

impl ContractCompiler {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            modules: Vec::new(),
            surface: None,
            surface_spec: None,
        }
    }

    pub fn modules(mut self, modules: impl IntoIterator<Item = Module>) -> Self {
        self.modules.extend(modules);
        self
    }

    /// Bind the concrete authoritative Surface once. Every compiler output
    /// uses this exact value and its compiled `SurfaceSpec`; no table inventory
    /// or caller-supplied unrelated Surface is accepted beside the manifest.
    pub fn with_surface(
        mut self,
        id: impl Into<String>,
        surface: impl Into<Arc<Surface>>,
    ) -> Result<Self, String> {
        let surface = surface.into();
        let spec = SurfaceSpec::from_surface(id, &surface).map_err(|error| error.to_string())?;
        if let Some(existing) = &self.surface_spec {
            if existing.id != spec.id || existing.fingerprint != spec.fingerprint {
                return Err(format!(
                    "ContractCompiler already has a different authoritative Surface contract"
                ));
            }
            return Err(
                "ContractCompiler accepts exactly one authoritative Surface contract".into(),
            );
        }
        self.surface = Some(surface);
        self.surface_spec = Some(spec);
        Ok(self)
    }

    /// Construct a compiler around one authoritative Surface contract.
    pub fn from_surface(
        name: impl Into<String>,
        surface_id: impl Into<String>,
        surface: impl Into<Arc<Surface>>,
    ) -> Result<Self, String> {
        Self::new(name).with_surface(surface_id, surface)
    }

    /// Return the already-bound shared Surface IR.
    pub fn surface(&self) -> Result<Surface, String> {
        self.surface
            .as_deref()
            .cloned()
            .ok_or_else(|| "ContractCompiler requires one bound Surface contract".into())
    }

    /// Render SDL directly from the contract-only Surface IR.
    pub fn graphql_sdl(&self) -> Result<String, String> {
        crate::graphql::graphql_sdl_from_surface(
            self.surface
                .as_deref()
                .ok_or_else(|| "ContractCompiler requires one bound Surface contract".to_owned())?,
        )
    }

    /// Compile the selected client artifact from the same Surface identity.
    pub fn client_manifest(&self) -> Result<DistributedClientManifest, ClientManifestError> {
        let surface = self.surface.clone().ok_or_else(|| {
            ClientManifestError("ContractCompiler requires one bound Surface contract".into())
        })?;
        let expected = self.bound_surface_spec().map_err(ClientManifestError)?;
        let export = DistributedClientSurfaceExport::from_contract(self.name.clone(), surface)?;
        let actual = SurfaceSpec::from_surface(expected.id.clone(), export.surface().as_ref())
            .map_err(|error| ClientManifestError(error.to_string()))?;
        if actual.id != expected.id || actual.fingerprint != expected.fingerprint {
            return Err(ClientManifestError(
                "client manifest Surface identity diverges from the compiler contract".into(),
            ));
        }
        export.manifest()
    }

    /// Compile the logical manifest without mounting a handler.
    pub fn manifest(&self) -> ApplicationResult<ApplicationManifest> {
        let manifest = ApplicationManifest::try_from_modules(
            self.name.clone(),
            self.modules.clone(),
            self.surface_spec.clone().into_iter(),
        )?;
        let expected = self
            .bound_surface_spec()
            .map_err(crate::application::ApplicationError::InvalidSpec)?;
        if manifest
            .surfaces
            .iter()
            .find(|surface| surface.id == expected.id)
            .is_none_or(|surface| surface.fingerprint != expected.fingerprint)
        {
            return Err(crate::application::ApplicationError::NonCanonical(
                "compiler Surface identity",
            ));
        }
        Ok(manifest)
    }

    pub fn compile(&self) -> ApplicationResult<Application> {
        let application = Application::try_new(
            self.name.clone(),
            self.modules.clone(),
            self.surface_spec.clone().into_iter(),
        )?;
        let expected = self
            .bound_surface_spec()
            .map_err(crate::application::ApplicationError::InvalidSpec)?;
        if application
            .manifest()
            .surfaces
            .iter()
            .find(|surface| surface.id == expected.id)
            .is_none_or(|surface| surface.fingerprint != expected.fingerprint)
        {
            return Err(crate::application::ApplicationError::NonCanonical(
                "compiler Surface identity",
            ));
        }
        Ok(application)
    }

    fn bound_surface_spec(&self) -> Result<SurfaceSpec, String> {
        self.surface_spec
            .clone()
            .ok_or_else(|| "ContractCompiler requires one bound Surface contract".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::surface::{build_surface, SurfaceOptions};
    use crate::table::{ColumnType, PrimaryKey, TableColumn, TableKind, TableSchema};

    fn table(model: &str, table: &str) -> TableSchema {
        TableSchema {
            model_name: model.into(),
            table_name: table.into(),
            columns: vec![TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            }],
            primary_key: PrimaryKey::new(["id"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    fn surface(models: &[(&str, &str)]) -> Arc<Surface> {
        let tables = models
            .iter()
            .map(|(model, name)| table(model, name))
            .collect::<Vec<_>>();
        Arc::new(build_surface(&tables, &SurfaceOptions::sqlite()).unwrap())
    }

    const UNBOUND: &str = "ContractCompiler requires one bound Surface contract";

    #[test]
    fn an_unbound_compiler_produces_nothing() {
        let compiler = ContractCompiler::new("contracts");
        assert_eq!(compiler.surface().unwrap_err(), UNBOUND);
        assert_eq!(compiler.graphql_sdl().unwrap_err(), UNBOUND);
        assert_eq!(compiler.client_manifest().unwrap_err().0, UNBOUND);
        assert_eq!(
            compiler.manifest().unwrap_err(),
            crate::application::ApplicationError::InvalidSpec(UNBOUND.into())
        );
        assert_eq!(
            compiler.compile().unwrap_err(),
            crate::application::ApplicationError::InvalidSpec(UNBOUND.into())
        );
    }

    #[test]
    fn a_compiler_binds_exactly_one_surface() {
        let todos = surface(&[("TodoView", "todos")]);
        let bound = ContractCompiler::from_surface("contracts", "web", Arc::clone(&todos)).unwrap();
        let again = ContractCompiler::from_surface("contracts", "web", Arc::clone(&todos))
            .unwrap()
            .with_surface("web", Arc::clone(&todos))
            .err();
        assert_eq!(
            again.as_deref(),
            Some("ContractCompiler accepts exactly one authoritative Surface contract")
        );
        let other = bound
            .with_surface("web", surface(&[("ChatView", "chats")]))
            .err();
        assert_eq!(
            other.as_deref(),
            Some("ContractCompiler already has a different authoritative Surface contract")
        );
    }

    #[test]
    fn compile_pins_the_application_to_the_bound_surface() {
        let todos = surface(&[("TodoView", "todos")]);
        let compiler = ContractCompiler::from_surface("contracts", "web", Arc::clone(&todos))
            .unwrap()
            .modules(Vec::new());
        assert_eq!(
            SurfaceSpec::from_surface("web", &compiler.surface().unwrap()).unwrap(),
            SurfaceSpec::from_surface("web", &todos).unwrap()
        );
        let application = compiler.compile().unwrap();
        assert_eq!(application.name(), "contracts");
        assert!(application.modules().is_empty());
        let expected = SurfaceSpec::from_surface("web", &todos).unwrap();
        assert_eq!(application.surfaces(), std::slice::from_ref(&expected));
        assert_eq!(application.manifest(), &compiler.manifest().unwrap());
        assert_eq!(
            application.fingerprint().unwrap(),
            application.manifest().fingerprint().unwrap()
        );
    }

    #[test]
    fn the_builder_records_extensions_and_provenance_in_the_fingerprint() {
        let spec = SurfaceSpec::from_surface("web", &surface(&[("TodoView", "todos")])).unwrap();
        let plain = Application::new("app")
            .surfaces([spec.clone()])
            .build()
            .unwrap();
        let extension =
            ApplicationExtension::try_new("ui", 1, serde_json::json!({"theme": "dark"})).unwrap();
        let provenance = ManifestProvenance {
            generator: "test".into(),
            source_revision: Some("git:abc".into()),
            sources: Vec::new(),
        };
        let annotated = Application::new("app")
            .surfaces([spec])
            .extensions([extension.clone()])
            .provenance(provenance.clone())
            .build()
            .unwrap();
        assert!(annotated.manifest().extensions.contains(&extension));
        assert_eq!(annotated.manifest().provenance, provenance);
        assert_ne!(
            annotated.fingerprint().unwrap(),
            plain.fingerprint().unwrap()
        );
        let copy = annotated.clone();
        assert_eq!(
            copy.canonical_bytes().unwrap(),
            annotated.canonical_bytes().unwrap()
        );
    }
}
