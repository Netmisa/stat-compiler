<?php
namespace CanalTP\StatCompiler\DependencyInjection;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\Reference;

/**
 * Compiler pass aiming to add all the updaters tagged in the DIC to the command
 */
class UpdaterCompilerPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container)
    {
        if ($container->hasDefinition('updatedb_command')) {
            $definition = $container->getDefinition(
                'updatedb_command'
            );

            $taggedServices = $container->findTaggedServiceIds(
                'updatedb.updater'
            );

            foreach ($taggedServices as $id => $attributes) {
                $definition->addMethodCall(
                    'addUpdater',
                    array(new Reference($id))
                );
            }
        }

        if ($container->hasDefinition('initdb_command')) {
            $definition = $container->getDefinition(
                'initdb_command'
            );

            $taggedServices = $container->findTaggedServiceIds(
                'initdb.updater'
            );

            foreach ($taggedServices as $id => $attributes) {
                $definition->addMethodCall(
                    'addUpdater',
                    array(new Reference($id))
                );
            }
        }
    }
}