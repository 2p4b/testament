defmodule Testament.Repo.Migrations.CreateEffectss do
    use Ecto.Migration

    def change do
        create table(:effects, primary_key: false) do
            add :uuid,      :uuid,      primary_key: true
            add :data,      :text
            add :namespace, :string
        end
    end
end
