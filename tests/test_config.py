from station_persist.config import Settings


def settings(url: str) -> Settings:
    return Settings(_env_file=None, database_url=url)


def test_remove_channel_binding_da_url_do_neon() -> None:
    url = (
        "postgresql://user:secret@ep-test-pooler.sa-east-1.aws.neon.tech/neondb"
        "?sslmode=require&channel_binding=require"
    )
    assert settings(url).database_url.get_secret_value() == (
        "postgresql://user:secret@ep-test-pooler.sa-east-1.aws.neon.tech/neondb"
        "?sslmode=require"
    )


def test_preserva_url_sem_opcoes_exclusivas_da_libpq() -> None:
    for url in (
        "postgresql://postgres:postgres@postgres:5432/agritech_test",
        "postgresql://u:p@host/db?sslmode=require&application_name=persist",
    ):
        assert settings(url).database_url.get_secret_value() == url
