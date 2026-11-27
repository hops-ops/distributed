use distributed::application::{Application, ApplicationManifest, SurfaceSpec};
use distributed::table::ReadModelCatalog;

use crate::read_models::ZeroConfigAppView;

pub fn application_manifest() -> ApplicationManifest {
    let catalog = read_model_catalog();
    let surface = distributed::graphql::build_surface(
        &catalog.tables,
        &distributed::graphql::SurfaceOptions::sqlite(),
    )
    .expect("read-model catalog should compile into an application Surface");
    let surface = SurfaceSpec::from_surface("zero-config-app", &surface)
        .expect("application Surface contract should be portable");
    Application::new("zero-config-app")
        .surface(surface)
        .build()
        .expect("application manifest should compile")
        .manifest()
        .clone()
}

pub fn read_model_catalog() -> ReadModelCatalog {
    ReadModelCatalog::new("zero-config-app").read_model::<ZeroConfigAppView>()
}

pub fn service_descriptor() -> distributed::microsvc::ServiceDescriptor {
    crate::service::descriptor()
}
